#!/usr/bin/env bash

: <<!
Name: test_signal-insert.sh
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-10 11:29:17

Description:

Attentions:
-

Depends:
-
!

pgbench --host=localhost --port=5432 --username=postgres -M simple -n -r -P 1 -f ./test.sql -C -c 1 -j 1 -T 120
