#!/bin/bash

M_user="hadoop"
M_hive_ip="192.168.174.160"

count=0
project_path=$(cd `dirname $0`; pwd)
for x in ` awk "{print $1}" ./table_name.txt `
do
  topic=$x
  count=`expr ${count} + 1`
  # 远程登录集群，建表
  ssh ${M_user}@${M_hive_ip} > /dev/null 2>&1 <<eeooff
  /data_b/module/kafka/bin/kafka-topics.sh --zookeeper hadoop104:2181,hadoop105:2181,collector101:2181 --create --replication-factor 1 --partitions 1 --topic ${topic}
eeooff
  echo "*** 创建第 ${count} 个主题  ${topic}  成功"
  echo ""
done


