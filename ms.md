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