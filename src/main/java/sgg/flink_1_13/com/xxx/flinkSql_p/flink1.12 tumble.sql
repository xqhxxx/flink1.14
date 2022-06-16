
-- （flink 1.12.1）场景：简单且常见的分维度分钟级别同时在线用户数、总销售额
create table source_table(
    dim string,
    user_id Bigint,
    price bigint,
    row_time as cast(current_timestamp as timestamp(3)),
    watermark for row_time as row_time -interval '5' second
)with(
    'connector'='datagen',
);

create  table sink_table(
    dim string,
    pv bigint,
    sum_price bigint,
    max_price bigint,
    min_price bigint,
    uv bigint,
    window_start bigint
)with (
    'connector'='print'
);

insert into sink_table
select dim,
       sum(bucket_pv) as pv,
       sum(bucket_sum_price) as sum_price,
       max(bucket_max_price) as max_price,
       min(bucket_min_price) as min_price,
       sum(bucket_uv) as uv,
       max(window_start) as window_start
from (
         select dim,
                count(*) as bucket_pv,
                sum(price) as bucket_sum_price,
                max(price) as bucket_max_price,
                min(price) as bucket_min_price,
                -- 计算 uv 数
                count(distinct user_id) as bucket_uv,
                //窗口聚合 tumble_start(row_time, interval '1' minute)
                cast(tumble_start(row_time, interval '1' minute) as bigint) * 1000
                    as window_start
         from source_table
         group by
             -- 按照用户 id 进行分桶，防止数据倾斜
             mod(user_id, 1024),
             dim,
             tumble(row_time, interval '1' minute)
     )
group by dim,window_start;



