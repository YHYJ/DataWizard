# CHANGELOG

CHANGELOG

---

## Table of Contents

<!-- vim-markdown-toc GFM -->

* [v0.8.3](#v083)
* [v0.8.2](#v082)
* [v0.8.1](#v081)
* [v0.8.0](#v080)
* [v0.7.3](#v073)
* [v0.7.2](#v072)
* [v0.7.1](#v071)
* [v0.7.0](#v070)
* [v0.6.2](#v062)
* [v0.6.1](#v061)
* [v0.6.0](#v060)
* [v0.5.2](#v052)
* [v0.4.0](#v040)
* [v0.3.0](#v030)
* [v0.2.0](#v020)
* [v0.1.2](#v012)
* [v0.1.1](#v011)
* [v0.1](#v01)

<!-- vim-markdown-toc -->

---

更新日志

---

## v0.8.3

- 提示clientid不能重复问题
- 调整日志参数
- 修改compose文件不使用PWD变量
- 从queue.Queue切换回multiprocessing.Queue: 多线程和多进程通信队列的区别

## v0.8.2

- 线程数减少问题已排除，现在使用新版入库函数
- 当配置了worker数时不再限制其最小值
- 将测试用的多线程方法合并后删除测试用例
- 修改插件防止其返回值可能是None

## v0.8.1

- 添加数据结构检查函数，通过检查的数据才会进入入库流程，不通过的则被放弃

## v0.8.0

- start_source和start_wizard分到两个进程运行
- 重写mqtt_wrapper，函数取代类

## v0.7.3

- mqtt_wrapper修改前最后一个tag

## v0.7.2

- 添加MQTT重连方法

## v0.7.1

- 补充log
- fixed bug

## v0.7.0

- `insert_nextgen`全面替换`insert`方法

## v0.6.2

- 调整worker数，并设置最小为CPU核心数+4
- 使worker数和tasks数保持一致

## v0.6.1

- 调整配置文件结构

## v0.6.0

- 全面从insert切换到insert_nextgen

## v0.5.2

- 添加版本号
- 增加对json数据的支持
- 加入插件系统
- 增加CHANGELOG

## v0.4.0

- 更新配置为呢键

## v0.3.0

- 修复一些小错误
- 准备改版

## v0.2.0

- 重构mqtt操作代码

## v0.1.2

- 提供compose文件

## v0.1.1

- 修复log记录问题

## v0.1

- 初步完成
