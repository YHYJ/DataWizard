--  测试多行插入：734.21 row/s
\set id random(1,10000000)
insert into t_good (id, crt_time) select random()*100000000,now() from generate_series(1,100) t(id);
