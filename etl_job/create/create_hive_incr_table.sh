#!/bin/bash
Y_ip="rr-bp1ex9u909eh66g4d.mysql.rds.aliyuncs.com"
Y_post="3306"
Y_user="tms_read"
Y_password="Tms_read"
Y_db="auth"
M_db="ods_incr"
M_user="root"
M_hive_ip="172.16.182.178"
project_path=$(cd `dirname $0`; pwd)
count=0

for x in ` awk "{print $1}" ./table_name.txt `
do
  count=`expr ${count} + 1`
  Y_table=$x
  M_table="ods_${Y_db}_${Y_table}_incr"
  # 获取hive的建表语句
  create_sql="
  SELECT
  concat(a.column_name, ' ', a.column_type, \" COMMENT'\", a.column_comment, \"',\") as createsql
  from(
  select
    column_name,
    column_comment,
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
  echo "****** HIVE *** THR ***获取 HIVE 的 最终 建表字段："
  echo ${createsql}
  echo ""

  # 构建hive建表语句
  create_hive="
  use ${M_db};
  CREATE TABLE IF NOT EXISTS ${M_table}(
  ${createsql}
  )
  clustered by(id) into 1 buckets
  ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  WITH SERDEPROPERTIES (
    'field.delim'='\u0001',
    'serialization.format'='\u0001')
  STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
  TBLPROPERTIES ('transactional'='true');
  truncate table ${M_table};
  "
  echo "****** HIVE *** FOUR ***拼接 HIVE 的建表语句  如果已经纯在，清空表结构："
  echo ${create_hive}
  echo ${create_hive} > ${project_path}/tms_ods_incr/${Y_table}.sql
  echo ""

  # 远程登录集群，建表
  ssh ${M_user}@${M_hive_ip} > /dev/null 2>&1 <<eeooff
  hive -e "${create_hive}" > /dev/null 2>&1
eeooff
  echo "*** HIVE *** FIVE ***表创建成功  OR  或者已经纯在, 并且清空表结构"
  echo "*** 创建第 ${count} 个主题  $M_table  成功"
  echo ""
done
