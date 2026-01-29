-- 第一天初始化
insert overwrite table dws_result
partition(pt_d='20251001')
select
    t1.imei as sn_code
   ,t1.activate_time
   ,t1.device_type
   ,t1.device_name
   ,t2.private_key
   ,t2.software_version
   ,t2.os_version
   ,1 as report_days
from
(
    select
        imei
       ,activate_time
       ,device_type
       ,device_name
    from ads
    where pt_d = '$date'
        and etl_day = '$date'
) t1
join
(
    select
        sn_code
       ,private_key
       ,max(software_version) as software_version
       ,max(os_version) as os_version
    from dwd
    where pt_d = '$date'
    group by
        sn_code
       ,private_key
) t2
on t1.imei = t2.sn_code

-- 第二天
-- 新激活的sn对应私钥
create table temp.tmp_new_sn
as
select
    t1.imei as sn_code
   ,t1.activate_time
   ,t1.device_type
   ,t1.device_name
   ,t2.private_key
   ,t2.software_version
   ,t2.os_version
   ,1 as report_days
from
(
    select
        imei
       ,activate_time
       ,device_type
       ,device_name
    from ads
    where pt_d = '$date'
        and etl_day = '$date'
) t1
join
(
    select
        sn_code
       ,private_key
       ,max(software_version) as software_version
       ,max(os_version) as os_version
    from dwd
    where pt_d = '$date'
    group by
        sn_code
       ,private_key
) t2
on t1.imei = t2.sn_code

-- 历史sn对应的新私钥
create table temp.tmp_history_sn_with_new_key
as
select
    t1.sn_code
   ,t3.activate_time
   ,t3.device_type
   ,t3.device_name
   ,t1.private_key
   ,t1.software_version
   ,t1.os_version
   ,1 as report_days
from
(
    select
        sn_code
       ,private_key
       ,max(software_version) as software_version
       ,max(os_version) as os_version
    from dwd
    where pt_d = '$date'
    group by
        sn_code
       ,private_key
) t1
left join
(
    select
        imei
       ,private_key
    from dws_result
    where pt_d = '$last_date'
) t2
on t1.sn_code = t2.imei
    and t1.private_key = t2.private_key
left join
(
    select
        imei
       ,max(activate_time) as activate_time
       ,max(device_type) as device_type
       ,max(device_name) as device_name
    from dws_result
    where pt_d = '$last_date'
    group by imei
) t3
on t1.sn_code = t3.imei
where t2.private_key is null
    and t1.sn_code in (
        select imei
        from dws_result
        where pt_d = '$last_date'
    )

-- 历史sn重复出现的私钥
create table temp.tmp_history_sn_with_duplicate_key
as
select
    t1.imei as sn_code
   ,t1.activate_time
   ,t1.device_type
   ,t1.device_name
   ,t1.private_key
   ,t1.software_version
   ,t1.os_version
   ,t1.report_days + if(t2.private_key is not null, 1, 0) as report_days
from
(
    select
        imei
       ,activate_time
       ,device_type
       ,device_name
       ,private_key
       ,software_version
       ,os_version
       ,report_days
    from dws_result
    where pt_d = '$last_date'
) t1
left join
(
    select
        sn_code
       ,private_key
    from dwd
    where pt_d = '$date'
    group by
        sn_code
       ,private_key
) t2
on t1.imei = t2.sn_code
    and t1.private_key = t2.private_key
;

-- 写入第二天的结果
insert overwrite table dws_result
partition(pt_d='20251002')
select
    sn_code
   ,activate_time
   ,device_type
   ,device_name
   ,private_key
   ,software_version
   ,os_version
   ,report_days
from temp.tmp_new_sn
union all
select
    sn_code
   ,activate_time
   ,device_type
   ,device_name
   ,private_key
   ,software_version
   ,os_version
   ,report_days
from temp.tmp_history_sn_with_new_key
union all
select
    sn_code
   ,activate_time
   ,device_type
   ,device_name
   ,private_key
   ,software_version
   ,os_version
   ,report_days
from temp.tmp_history_sn_with_duplicate_key
