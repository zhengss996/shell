#!/bin/bash

project_path=$(cd `dirname $0`; pwd)

y_db='datax'
y_table='person'
m_db='datax'
m_table="ods_${y_db}_${y_table}_full"


bash  ${project_path}/${m_table}.sh \
mysql 192.168.174.132    3306      ${y_db}        ${y_table}      root     root \
hive  192.168.174.160    ${m_db}   ${m_table}     orc             /hive/warehouse \
hdfs  192.168.174.160    hadoop    /usr/local/hadoop/bin \
${project_path} > ${project_path}/${m_table}_`date +%Y%m%d%H%M%S`.log 2>&1 &


# 注意：
#     hive 的 ip 只要配置了免密登录，集群中任何一台都可以（namenode、datanode）
#     hdfs 的 ip 必须时 active， 不能是 standby

# json 高可用配置
#     dfs.nameservices：在hadoop目录下的 hdfs-site.xml 文件中
#     dfs.ha.namenodes.testDfs: 逻辑名称 --> 同上
#     dfs.namenode.rpc-address.aliDfs.namenode1： rpc地址 --> 同上
