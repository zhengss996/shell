#!/bin/bash

# 说明：
# 此脚本用于创建hive和hbase的关联表
# 通过读取mysql的元数据库得到mysql表的字段名和字段类型，通过case when 映射得到hive的字段类型，最终组合得到hive的建表语句，远程登录集群建表
# hbase的命名空间和hive的库名保持一致


Y_ip="192.168.174.132"
Y_post="3306"
Y_user="root"
Y_password="root"
Y_db="datax"
M_db="datax"
M_user="hadoop"
M_hive_ip="192.168.174.160"

project_path=$(cd `dirname $0`; pwd)
count=0

for x in ` awk "{print $1}" ./table_name.txt `
do
  count=`expr ${count} + 1`
  Y_table=$x
  M_table="ods_${Y_db}_${Y_table}_incr"

  # 获取hive的建表字段语句
  create_sql="
  SELECT
  concat(a.column_name, ' ', a.column_type, \",\") as createsql
  from(
  select
    column_name,
    case
      when column_type like 'datetime' then 'TIMESTAMP'
      when column_type like 'timestamp%' then 'TIMESTAMP'
      when column_type like 'date%' then 'date'
      when column_type like 'decimal%' then 'FLOAT'
      when column_type like 'double%' then 'double'
      when column_type like 'varchar%' then column_type
      when column_type like 'int%' then 'INT'
      when column_type like 'bigint%' then 'bigint'
      when column_type like 'tinyint%' then 'TINYINT'
      when column_type like 'text%' then 'STRING'
      when column_type like 'longtext%' then 'STRING'
      when column_type like 'char%' then 'STRING'
      else '' end as column_type
  FROM information_schema.columns
  WHERE table_schema= \"${Y_db}\" AND table_name = \"${Y_table}\"
  ) a
  "
  echo "****** HIVE *** ONE ***获取 HIVE 建表字段的 SQL 语句："
  echo ${create_sql}
  echo ""

  DATA="`mysql -h${Y_ip} -p${Y_post} -u${Y_user} -p${Y_password} <<EOF
  ${create_sql}
EOF`"
  echo "****** HIVE *** TOW ***获取 HIVE 的建表字段："
  echo ${DATA}
  echo ""

  # 获取字符串长度
  length=`expr ${#DATA} - 10`
  # 获取字段属性
  createsql=${DATA:9:${length}}
  echo "****** HBASE *** THR ***获取 HBASE 的 最终 映射字段："
  echo ${createsql}
  echo ""

  # 获取hbase的建表映射字段
  create_sql1="
  select
    concat('info:', column_name, ',') as createsql
  FROM information_schema.columns
  WHERE table_schema= \"${Y_db}\" AND table_name = \"${Y_table}\"
  "
  echo "****** HBASE *** ONE ***获取 HBASE 建表字段的 映射 语句："
  echo ${create_sql1}
  echo ""

  DATA1="`mysql -h${Y_ip} -p${Y_post} -u${Y_user} -p${Y_password} <<EOF
  ${create_sql1}
EOF`"
  echo "****** HBASE *** TOW ***获取 HBASE 的建表字段："
  echo ${DATA1}
  echo ""

  # 获取字符串长度
  length=`expr ${#DATA1} - 10`
  # 获取字段属性
  createsqlw=${DATA1:9:${length}}
  echo "****** HBASE *** THR ***获取 HBASE 的 最终 映射字段："
  echo ${createsqlw}
  echo ""

  single="'"

  # 构建hive建表语句
  create_hive="
  use ${M_db};
  CREATE TABLE IF NOT EXISTS ${M_table}(
  key string,
  ${createsql}
  )
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES ('hbase.columns.mapping' = ${single}:key,${createsqlw}${single})
  TBLPROPERTIES ('hbase.table.name' = ${single}${M_db}:${M_table}${single});"
  
  echo "****** HIVE *** FOUR ***拼接 HIVE 的建表语句  如果已经纯在，清空表结构："
  echo ${create_hive}
  echo ${create_hive} > ${project_path}/table/${Y_table}.sql
  echo ""

  # 远程登录集群，建表
  ssh ${M_user}@${M_hive_ip} > /dev/null 2>&1 <<eeooff
  hive -e "${create_hive}" > /dev/null 2>&1
eeooff
  echo "*** HIVE *** FIVE ***表创建成功  OR  或者已经纯在, 并且清空表结构"
  echo "*** 创建第 ${count} 个主题  $M_table  成功"
  echo ""
done
