#!/usr/bin/env bash

: <<!
Name: test_multi-insert.sh
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-10 11:31:08

Description: 用于测试TimescaleDB的多行插入

Attentions:
-

Depends:
-
!

pgbench --host=localhost --port=5432 --user=postgres -M prepared -n -r -P 1 -f ./muilt_insert.sql -c 32 -j 32 -T 120
