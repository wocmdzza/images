# 大数据开发面试宝典（SQL + Spark 离线数仓）
## 目录

- [一、SQL 高频面试题](#一sql-高频面试题)
  - [1. 漏斗转化率统计](#1-漏斗转化率统计)
  - [2. 好友推荐与共同好友](#2-好友推荐与共同好友)
  - [3. 连续登录天数统计](#3-连续登录天数统计)
- [二、大数据开发场景题](#二大数据开发场景题)
  - [4. 线上 SQL 执行缓慢的排查与优化](#4-线上-sql-执行缓慢的排查与优化)
- [三、Spark 离线数仓高频面试点](#三spark-离线数仓高频面试点)
  - [核心原理](#核心原理)
  - [Shuffle 优化](#shuffle-优化)
  - [Spark SQL 优化](#spark-sql-优化)
  - [存储与格式](#存储与格式)
  - [参数与调优实践](#参数与调优实践)

## 一、SQL 高频面试题
### 1. 漏斗转化率统计

正确 SQL（严格漏斗口径）
```sql
WITH user_daily AS (
    SELECT
        to_date(behavior_time) AS dt,
        user_id,
        MAX(CASE WHEN behavior = 'click' THEN 1 ELSE 0 END) AS has_click,
        MAX(CASE WHEN behavior = 'cart'  THEN 1 ELSE 0 END) AS has_cart,
        MAX(CASE WHEN behavior = 'pay'   THEN 1 ELSE 0 END) AS has_pay
    FROM user_behavior
    GROUP BY to_date(behavior_time), user_id
)
SELECT
    dt,
    SUM(has_click) AS click_users,
    SUM(has_click * has_cart) AS cart_users,                -- 点击后加购
    SUM(has_click * has_cart * has_pay) AS pay_users,      -- 点击加购后支付
    ROUND(SUM(has_click * has_cart) / NULLIF(SUM(has_click), 0), 4) AS click_to_cart_rate,
    ROUND(SUM(has_click * has_cart * has_pay) / NULLIF(SUM(has_click * has_cart), 0), 4) AS cart_to_pay_rate
FROM user_daily
GROUP BY dt
ORDER BY dt;
```

要点
- 漏斗要求前后步骤必须关联，加购用户必须是点击过的那批人
- 用 MAX(CASE WHEN) 将行为转为 0/1 标记，相乘即可得到严格漏斗人数
- NULLIF 避免除以零

### 2. 好友推荐与共同好友
表结构
```sql
t_user (user_id, user_name)
t_friend (user_id, friend_id)   -- 单向存储
```

要求
为每个用户推荐可能认识的人：有共同好友且还不是好友
输出：user_id, 推荐好友ID, 共同好友数，按用户ID升序、共同好友数降序

正确 SQL
```sql
WITH bidirectional_friend AS (
    SELECT user_id, friend_id FROM t_friend
    UNION ALL
    SELECT friend_id AS user_id, user_id AS friend_id FROM t_friend
),
common_friends AS (
    SELECT
        a.user_id,
        b.user_id AS recommended_id,
        COUNT(DISTINCT a.friend_id) AS common_cnt
    FROM bidirectional_friend a
    JOIN bidirectional_friend b
      ON a.friend_id = b.friend_id    -- 共同好友
     AND a.user_id < b.user_id        -- 避免重复对
    WHERE NOT EXISTS (
        SELECT 1 FROM t_friend f
        WHERE (f.user_id = a.user_id AND f.friend_id = b.user_id)
           OR (f.user_id = b.user_id AND f.friend_id = a.user_id)
    )                                 -- 排除已是好友
    GROUP BY a.user_id, b.user_id
)
SELECT user_id, recommended_id, common_cnt
FROM common_friends
UNION ALL
SELECT recommended_id AS user_id, user_id AS recommended_id, common_cnt
FROM common_friends
ORDER BY user_id, common_cnt DESC;
```

要点
- 由于原关系表单向存储，先补全为双向
- 自连接通过共同好友关联，COUNT(DISTINCT a.friend_id) 即为共同好友数
- 使用 NOT EXISTS 排除已存在的好友关系（需考虑双向）
- 最后展开为每个用户一条推荐记录

### 3. 连续登录天数统计
表结构
```sql
user_id STRING,
login_date DATE
```

要求
找出连续登录天数 ≥ 3 的所有用户，输出：user_id、起始日期、结束日期、连续天数，每段连续登录都输出

正确 SQL
```sql
WITH dedup AS (
    SELECT DISTINCT user_id, login_date FROM user_login
),
ranked AS (
    SELECT
        user_id,
        login_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn
    FROM dedup
),
base_date AS (
    SELECT
        user_id,
        login_date,
        DATE_SUB(login_date, rn) AS grp_date   -- 连续标志
    FROM ranked
),
continuous AS (
    SELECT
        user_id,
        grp_date,
        MIN(login_date) AS start_date,
        MAX(login_date) AS end_date,
        COUNT(*) AS continuous_days
    FROM base_date
    GROUP BY user_id, grp_date
    HAVING COUNT(*) >= 3
)
SELECT user_id, start_date, end_date, continuous_days
FROM continuous
ORDER BY user_id, start_date;
```

要点
- 核心思想：连续日期减去递增序号会得到一个相同的基准日期
- 先对日期去重，防止同一天多条记录破坏连续性
- 按用户和基准日期分组，MIN/MAX 得到起止日期，COUNT 为连续天数

## 二、大数据开发场景题
### 4. 线上 SQL 执行缓慢的排查与优化
场景：离线数仓任务平时 30 分钟，某天跑了 2 小时未结束，排查思路
排查思路框架（按优先级）：
1. 代码与参数变更
    - 检查 SQL 是否被修改（Git/Scheduler diff）
    - 检查任务参数是否有调整（Shuffle 分区数、广播阈值、AQE 开关等）
2. 数据量变化
    - 分区行数、存储大小是否暴增
    - 是否存在大量小文件
    - 统计信息是否过期（ANALYZE TABLE 未更新）
3. 资源与集群环境
    - YARN/K8s 队列是否有其他高优任务抢占资源
    - 节点是否有故障（磁盘慢、网络带宽占满）
    - External Shuffle Service 是否正常
4. 执行计划变化
    - 对比正常时与当前执行计划（EXPLAIN）
    - JOIN 策略是否由 Broadcast 退化为 SortMerge
    - AQE 是否改变了计划
5. 数据倾斜
    - Spark UI 中 Stage 的 Task 执行时间是否严重不均（长尾）
    - 大表 JOIN 大表时关联键分布不均
    - GROUP BY 倾斜
6. 存储与格式
    - 是否分区表未被裁剪（谓词下推失效）
    - 文件格式是否为列存，压缩是否开启

常见优化手段
- 数据倾斜：拆分倾斜 key、加盐打散、AQE 自动倾斜优化
- 广播失效：调大 spark.sql.autoBroadcastJoinThreshold
- 小文件：AQE 合并分区、写入前 DISTRIBUTE BY、后置合并任务
- 资源不足：增加 Executor 内存/核数，开启动态资源分配
- Shuffle 过大：开启 AQE，调整 spark.sql.shuffle.partitions

## 三、Spark 离线数仓高频面试点
### 核心原理
#### 1. 宽依赖与窄依赖
- 窄依赖：父 RDD 分区最多被子 RDD 一个分区使用（map/filter/co-partition join），可流水线执行
- 宽依赖：父分区被多个子分区使用（groupByKey/join 未 co-partition），产生 Shuffle，划分 Stage 边界
- 区分意义：调度与容错效率不同

### 2. Job / Stage / Task 划分
- 一个 Action 算子 → 一个 Job
- 宽依赖 → Stage 边界，Stage 内全为窄依赖，形成 pipeline
- 每个 Stage 内，一个分区 → 一个 Task

### 3. 内存管理模型
- Execution 内存：Shuffle/Join/Sort/Agg 临时数据
- Storage 内存：缓存 RDD/广播变量
- 统一内存管理（1.6+）允许 Execution 与 Storage 动态互借
- 关键参数：spark.memory.fraction（0.6）、spark.memory.storageFraction（0.5）

### Shuffle 优化
#### 4. 三种 Shuffle 实现
- Hash Shuffle（淘汰）：小文件数量 M×R，I/O 高
- Sort Shuffle：M 个数据文件 + M 个索引文件，合并后写入，减少小文件
- Tungsten-Sort Shuffle：堆外内存 + 序列化，避免 GC，当前默认最优

#### 5. groupByKey vs reduceByKey
- reduceByKey 在 Map 端做预聚合（Combine），大幅减少 Shuffle 数据量。
- groupByKey 全量 Shuffle，易 OOM，应避免使用

#### 6. AQE 核心功能
- 自动合并小分区（Coalesce Shuffle Partitions）
- 动态切换 JOIN 策略（SortMerge → Broadcast）
- 自动处理数据倾斜（拆分倾斜分区）

#### 7. 大表 JOIN 大表优化
- 预过滤与预聚合缩小数据量
- 避免 NULL 值参与 JOIN
- 倾斜处理：AQE 自动或手动打散
- 分桶（Bucket）表可避免 Shuffle

#### 8. 小文件问题
- 产生原因：动态分区写入 Task 过多
- 解决：AQE 合并分区、设置合适的 Shuffle 分区数、DISTRIBUTE BY 控制写入、后置合并、定期 archive

#### 9. 广播变量
- 将小表只读数据分发到每个 Executor 内存，Task 共享
- 自动广播阈值：spark.sql.autoBroadcastJoinThreshold（默认 10MB）
- 大表 JOIN 小表手动 hint：/*+ BROADCAST(t) */

#### 10. Catalyst 优化器流程
- 解析 → 分析 → 逻辑优化（谓词下推、列裁剪）→ 物理计划（CBO 选最优）→ 代码生成（Whole-Stage Codegen）

### 存储与格式
#### 11. Parquet vs ORC

均为列存、高压缩、支持下推。

Parquet 嵌套结构支持好，Spark 默认，生态广。

ORC 针对 Hive 优化，支持事务、索引。

选择：Spark 为主选 Parquet，纯 Hive 且需 ACID 可选 ORC。

#### 12. Bucket 分桶表

按 Key Hash 分桶，同 Key 在同一桶。

用于 Bucket Join（无需 Shuffle）、SMB Join（最高效）、快速抽样。

注意：桶数合理，写入时保证分桶属性。

### 参数与调优实践
#### 13. Spark SQL 慢查询系统调优步骤

定位瓶颈：Spark UI → Stage → Task（CPU/IO/Shuffle）。

并行度：调整 spark.sql.shuffle.partitions。

广播阈值：autoBroadcastJoinThreshold。

内存：Executor 内存，storageFraction。

AQE 开启：spark.sql.adaptive.enabled=true。

SQL 逻辑：谓词下推、列裁剪、避免 SELECT *、reduceByKey。

数据：处理倾斜、小文件、更新统计信息。

存储：列存 + 压缩，合理分区。

#### 14. 动态资源分配

根据负载自动增减 Executor。

需要 External Shuffle Service 保证 Executor 释放后数据仍可读。

提高资源利用率，多租户友好。

### 数据治理
#### 15. 数据治理框架与数仓分层
- **定义**：数据治理是对数据资产进行规范化管理、控制和提升的全过程，涵盖组织、制度、流程和技术，目标是保证数据的**可用性、一致性、完整性、安全性**。
- **数仓分层落地**：
  - **ODS（贴源层）**：原始数据入湖，保持源系统结构，负责数据接入、增量/全量策略、格式统一。
  - **DWD（明细层）**：数据清洗、标准化、去重、脱敏，确保数据质量基线。
  - **DWS（汇总层）**：面向分析主题轻度汇总，构建公共指标，避免下游重复计算。
  - **ADS（应用层）**：面向业务场景的报表、标签、接口，控制权限和生命周期。
- **治理手段**：元数据管理、数据血缘、数据质量监控、生命周期管理、命名规范、权限管控、审计日志。

#### 16. 数据血缘管理
- **定义**：数据血缘描述了数据从产生、加工流转到最终消费的完整链路，包括表级、字段级血缘。
- **作用**：影响分析、问题排查、溯源审计、数据价值评估。
- **Spark 离线数仓中采集方案**：
  - **解析执行计划**：通过 Spark 的 `QueryExecutionListener` 或 Hook 机制在 SQL 执行前后获取 Logical Plan，提取输入输出表和字段依赖。
  - **日志解析**：从 Spark History Server 或调度系统的日志中提取 Input/Output 信息。
  - **第三方工具**：如 Apache Atlas、DataHub，通过 Spark Agent 或 Hook 自动捕获血缘。
- **常见展示**：在数据地图中以 DAG 图展示表与任务的上下游关系，支持点击查看字段流转。

#### 17. 数据质量监控体系
- **分层监控**：
  - **ODS 层**：数据量波动（环比/同比）、延迟率、空值率、格式异常率、数据接入延迟。
  - **DWD/DWS 层**：主键唯一性、外键引用完整性、枚举字段合法性、金额/数量非负检查、去重。
  - **ADS 层**：业务指标合理性（如订单量环比不超过 50%）、与业务系统对账。
- **技术实现**：
  - **规则引擎**：编写 DQC SQL 规则（如 `SELECT COUNT(*) FROM table WHERE col IS NULL`），通过调度系统定时执行。
  - **基线告警**：基于历史数据设置动态阈值（如过去 7 天均值 ± 30%），超出则钉钉/邮件告警。
  - **阻断机制**：核心任务依赖质量检查节点，质量不通过则自动阻断下游，防止脏数据扩散。
- **Spark 优化**：质量检查脚本也需优化，避免全表扫描，利用分区过滤、列存裁剪，运行时间控制在分钟级。

#### 18. 元数据管理与数据地图
- **元数据分类**：
  - **技术元数据**：表结构、分区信息、存储位置、文件格式、大小、行数、创建/更新时间、Owner。
  - **业务元数据**：中文名称、业务含义、指标口径、维度/指标标签、数据域、主题域。
  - **操作元数据**：任务依赖、运行频率、平均耗时、最近运行状态、产出时间。
- **构建数据地图**：
  - 采集层：通过 Spark Hook、Scheduler API、Hive Metastore API 等收集元数据。
  - 存储层：存入 MySQL/ES/图数据库。
  - 服务层：提供搜索、标签、评价、关联分析（血缘、热度、相似表）功能。
- **价值**：让数据使用者快速找到可信数据，减少沟通成本。

#### 19. 数据生命周期与成本治理
- **冷热分层存储**：
  - 热数据（最近 3 个月）存 SSD/高性能集群，冷数据（3 个月以上）迁至对象存储（S3/COS）或低配 HDFS。
  - Spark 读取冷数据时可接受较高延迟但节约成本。
- **数据留存策略**：
  - 按分区设置保留周期（如 ODS 保留 1 年，DWD 保留 3 年，ADS 按需保留）。
  - 自动化 TTL 清理脚本，结合调度定期删除过期分区。
- **表压缩与重建**：
  - 定期对历史分区进行文件合并和重压缩（减少小文件、使用更高压缩率算法如 Zstd）。
  - 对查询频率极低的宽表进行列裁剪存储或直接归档下线。
- **成本归因**：统计每张表的存储成本、计算成本（ETL 耗时 × 资源单价），推动业务方优化或下线无用数据。

#### 20. 数据安全与权限控制
- **多租户隔离**：不同业务线使用不同 Hive 数据库或命名空间，队列隔离。
- **认证授权**：
  - 集成 Kerberos/LDAP 进行用户认证。
  - 使用 Ranger/Sentry 进行库/表/列/分区级细粒度授权（如敏感列脱敏、行级过滤）。
- **数据脱敏**：
  - 在 DWD 层对姓名、身份证、手机号等通过 Spark UDF 脱敏（加掩、哈希、截断）。
  - 动态脱敏：基于视图或策略，不同角色看到不同明文/掩码。
- **审计日志**：记录谁在何时读取/写入了哪些表，用于安全审计和异常行为检测。
- **Spark 作业侧**：在提交作业时传递用户身份（proxy user），确保计算引擎以提交者权限访问数据。

#### 21. 数据标准与模型规范
- **命名规范**：
  - 表名：`{层级}_{主题域}_{业务含义}_{粒度}`，如 `dwd_trade_order_detail_di`（交易域订单明细增量表）。
  - 字段名：全小写下划线，公共字段统一（如 `user_id`、`gmt_create`），避免混淆。
- **模型规范**：
  - 数仓分层遵循 ODS-DWD-DWS-ADS，禁止反向依赖。
  - 维度建模采用星型模型，事实表与维度表通过代理键关联。
  - 维表必须定义主键和拉链策略，事实表声明粒度。
- **落地方式**：
  - 嵌入模板与脚手架工具，新建表时必须填写元信息。
  - 上线前通过自动化脚本扫描建表语句是否符合规范。
  - 定期评审与通报不合规的表。

#### 22. 小文件治理专题
- **产生根源**：动态分区下并行写入过多 Task、频繁增量更新、未合并直接落盘。
- **预防策略**：
  - 写入时开启 AQE 合并：`spark.sql.adaptive.coalescePartitions.enabled=true`。
  - 调整 `spark.sql.shuffle.partitions`，避免远大于实际输出分区数。
  - 使用 `DISTRIBUTE BY partition_key` 让每个分区由少数 Task 写入。
  - 强制使用 `REBALANCE` hint 在写入前重分区。
- **事后治理**：
  - 离线合并任务：定期运行 `INSERT OVERWRITE` 目标分区，将小文件合并成大文件。
  - 使用 `hive.merge.mapfiles/mergeteardownfiles` 参数自动合并小文件（MR 引擎）。
  - 对于不再更新的冷分区，手动合并并转为只读存储。
- **监控**：每日统计每张表的小文件数，超过阈值告警并自动触发合并。

#### 23. 数据倾斜治理专题
- **预防**：
  - 设计时避免使用容易倾斜的关联键（如 null、业务大客户 ID）。
  - 使用分桶表且关联键同分布。
  - 在 DWD 层提前打散超大维度或进行预聚合。
- **运行时自动优化**：
  - 开启 AQE 自动倾斜处理：`spark.sql.adaptive.skewJoin.enabled=true`，配置倾斜因子与阈值。
  - 开启 `spark.sql.adaptive.localShuffleReader` 配合 AQE。
- **手动干预**：
  - 对确认的倾斜 Key 执行拆分逻辑：倾斜侧加随机后缀膨胀，小表侧对应复制 N 份，JOIN 后还原。
  - 两阶段聚合：先局部聚合（加盐），再去盐全局聚合。
  - 使用 `Broadcast` 小表避免 Shuffle Join。
- **事后复盘**：
  - 从 Spark UI 中找出倾斜 Stage，统计倾斜 Key 的分布，反馈给数据提供方优化源头。

#### 24. 治理指标与效果衡量
- **质量指标**：
  - 数据质量合格率（每天 DQC 规则通过率）。
  - 数据产出及时率（任务按时完成百分比）。
  - 数据误差率（与业务系统对账差异）。
- **成本指标**：
  - 存储总量及冷数据占比，存储成本月环比。
  - 计算资源消耗（任务总 vCore·h），无效计算占比（临时查询、重复计算）。
- **价值指标**：
  - 数据资产覆盖率（已纳入元数据管理的表占比）。
  - 数据热度（月访问次数，区分读/写）。
  - 数据复用率（一张表被下游任务引用的数量，衡量模型公共度）。
- **管理指标**：
  - 需求满足率、平均找数时间、数据问题平均解决时间。
  - 规范符合率（表命名、字段类型合规比例）。
