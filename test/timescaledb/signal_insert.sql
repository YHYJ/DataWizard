--  测试单行插入：57.65 row/s
\set id random(1,10000000)
insert into t_bad (id, crt_time) values (:id, now());
