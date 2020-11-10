# README

将数据从缓存持久化到数据库

---

## Table of Contents

<!-- vim-markdown-toc GFM -->

* [MQTT](#mqtt)
  * [Topic](#topic)
* [DataWizard](#datawizard)

<!-- vim-markdown-toc -->

---

<!-- Object info -->

---

## MQTT

### Topic

- 每个设备一个

  > 暂定和表名一致，例如'device_ATLAS_01'或者'device/ATLAS/01'

## DataWizard

1. 按设备从MQTT订阅数据
2. 使用批量INSERT
3. 有两种数据分批方案：
  1. 按时间分片
    主要可能出现数采频率不高导致一定时间片（例如1s，再长可能影响数据实时性）内只有一条数据，结果还是单条INSERT
  2. 按队列大小
    如果queue.qsize()到了一定大小，则采集这批数据，但可能出现达到指定大小耗时过长的问题
