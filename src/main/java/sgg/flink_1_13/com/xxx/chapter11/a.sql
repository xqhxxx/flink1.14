--tvf  表值函数

--DDL
create  table clickTable(user_name string,url,st string,ts bigint,
et as to_timestamp(from_unixtime(ts/1000)),
watermark for et as et- interval '1' second)
with ('connector'='filesystem','path'='input/click.csv','format'='csv')

--tumble w
select user_name, count(1) as cnt, window_end as endT
from table(tumble(table clickTable,descriptor(et),interval '10' second))
group by user_name, window_end, window_start;

select user_name, count(user_name) as cnt, window_end as endT
from TABLE(tumble(table clickTable,descriptor(et),interval '1' hour))
group by user_name, window_end, window_start;

select c1,c2,count(1)as cnt ,windpw_end as endT from
table( tumble( table c_t,descriptor(et),interval '2'  minute ) )
group by c1,window_end,window_start;