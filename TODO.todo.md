- [ ] 将创建普通表和超表的方法合并 (2021-04-25 12:42)
- [ ] 多线程创建table/column会导致卡在创建过程 (2020-12-31 16:43)
- [ ] 测试一下psycopg2自带的pool (2020-12-31 16:57)
- [ ] 修改main.py，符合批量插入要求 (2020-11-11 11:51)
  - [ ] 按时间片或队列大小 (2020-11-11 11:52)
- [X] 将columns的拼接方式改为','join(list) (2020-11-18 19:28)
- [X] 分流log信息 -- 实时数据里一份，总的log表里一份（recordtime，deviceid，level，message） (2020-11-06 17:51)
  - [X] 在insertData方法中检索log信息 (2020-11-12 10:32)
  - [X] 在main.py中另起一个进程，专门fork log (2020-11-12 10:33)
- [X] ! 启用fork_log标志位 (2020-12-28 11:24)
- [X] ! 解决data类型为dict的情况下log信息检索不全的问题 (2020-12-28 09:37)
- [X] ! 重构'insert'方法，使其只负责执行`INSERT`语句，不包括构建SQL语句的功能 (2021-04-08 13:52)
- [X] 取消insert函数，将功能全部转移到insert_nextgen函数 (2021-04-12 08:40)
  - [X] 还差错误处理（缺schema/table/column时的处理） (2021-04-12 08:41)
  - [X] 主要功能执行SQL已完成 (2021-04-12 08:40)
- [X] ! 验证MQTT是否需要断线重连机制 (2021-05-25 14:06)
- [X] ! 存活线程数在降低，并且在降低前打印了多次__on_message [168]: received message topic: device_monitor/data (2021-05-26 10:55)
- [X] !!! 插件解析可能有问题导致线程数量持续降低，暂时使用旧版！ (2021-06-04 13:30)
  - [X] 线程数量降低是原始数据格式不对导致的，现已增加数据格式检查函数
