--flink sql 提供的丰富的 join 方式（总结 6 种：
-- regular join，维表 join，temporal（快照） join，interval join，array 列转行，table function udtf函数

--left join，right join，full join 会存在着 retract 的问题，使用前，充分了解其运行机制，避免出现数据发重，发多的问题

--实时数仓中，regular join 以及 interval join，以及两种 join 的结合使用是最常使用


--hive 写法
insert  into sink_tb select a.*,b.* from a left join b on a.id=b.id
--flink   这样写的是retract 流 导致写入kafka数据变多 期望append流

--总结
--     1、inner join 会互相等，直到有数据才下发。
--     2、left join，right join，full join 不会互相等，只要来了数据，会尝试关联，能关联到则下发的字段是全的，
--     关联不到则另一边的字段为 null。后续数据来了之后，发现之前下发过为没有关联到的数据时，就会做回撤，把关联到的结果进行下发

---interval join  就是用一个流的数据去关联另一个流的一段时间区间内的数据。
--     关联到就下发关联到的数据，关联不到且在超时后就根据是否是 outer join（left join，right join，full join）下发没关联到的数据。
--     outer join 会输出没有 join 到的数据，inner join 会从 state 中删除这条数据

--前后 10 分钟之内的数据进行关联
insert  into sink_tb select a.*,b.* from a left join b on a.id=b.id
and a.row_time between b.row_time - interval '10' minute
    and b.row_time + interval  '10' minute
--时间区间 建议离线数据join并做时间戳diff比较，来确定时差


----lookup join  维表    -flatmap
-- 生产环境可以用 mysql，redis，hbase 来作为高速维表存储引擎
--实时更新、周期更新

insert into s_tb select a.* ,b.*
from a left join b for system_time as of a.proctime as b
on a.id=b.id
--使用for SYSTEM_TIME as of 时态表的语法来作为维表关联的标识语法
--使用dataStream优化方案   redis+local cache、异步访问外存、批量访问外存（redis pipeline）

--flink sql优化  1 、2官方hbase实现了、3批量官方无
