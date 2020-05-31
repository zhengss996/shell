#!/bin/bash

project_path=$(cd `dirname $0`; pwd)
echo ${project_path}

# 属性   ip       post     DB      TB         USER    PASSWORD     compress      默认路径
bash  ${project_path}/t_bse_customer_ods.sh \
mysql 127.0.0.1   3306    datax   person      root     root \
hive  192.168.174.162     datax   mysql2hive                        orc         /hive/warehouse \
hdfs  192.168.174.160                         hadoop                            /usr/local/hadoop/bin \
${project_path} > ${project_path}/t_bse_customer_ods_`date +%Y%m%d%H%M%S`.log 2>&1 &


# 注意：
#     hive 的 ip 只要配置了免密登录，集群中任何一台都可以（namenode、datanode）
#     hdfs 的 ip 必须时 active， 不能是 standby

# json 高可用配置
#     dfs.nameservices：在hadoop目录下的 hdfs-site.xml 文件中
#     dfs.ha.namenodes.testDfs: 逻辑名称 --> 同上
#     dfs.namenode.rpc-address.aliDfs.namenode1： rpc地址 --> 同上
