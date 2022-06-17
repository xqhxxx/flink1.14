-- 去重不仅仅有 count distinct 还有强大的 deduplication

--离线 hive sql的row_number=1

select id,timestamp ,page,p1,p2,pn
from (
    select id,timestamp ,page,p1,p2,pn
    --proctime 代表处理时间 即source表的proctime（）
    row_number() over(partition by id order by proctime)as rm
    from source_table
     )
where rn=1
-- deduplication 算子为每一个 partition key 都维护了一个 value state 用于去重。如果当前 value state 不为空，
-- 则说明 id 已经来过了，当前这条数据就不用下发了。如果 value state 为空，则 id 还没还没来过，把 value state 标记之后，把当前数据下发。
