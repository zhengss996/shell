#!/bin/bash

Y_ip=${2}
Y_post=${3}
Y_db=${4}
Y_table=${5}
Y_user=${6}
Y_password=${7}
echo "${1} 配置："  'Y_ip='${Y_ip} 'Y_post='${Y_post} 'Y_db='${Y_db}  'Y_table='${Y_table} 'Y_user='${Y_user} 'Y_password='${Y_password}

M_hive_ip=${9}
M_db=${10}
M_table=${11}
M_type=${12}
M_hive_warehouse=${13}
echo "${8} 配置："  'M_hive_ip='${M_hive_ip} 'M_db='${M_db} 'M_table='${M_table} 'M_type='${M_type} 'M_hive_warehouse='${M_hive_warehouse}

M_hdfs_ip=${15}
M_user=${16}
M_hadoop_address=${17}
M_addr=${18}
echo "${14} 配置："  'M_hdfs_ip='${M_hdfs_ip} 'M_user='${M_user} 'M_hadoop_address='${M_hadoop_address} 'M_addr='${M_addr}




# 获取json语句
create_json="
SELECT
	concat('{', '\"name\":\"', a.column_name, '\", \"type\":\"', data_type,'\"},') as datatype
FROM (
	select
	column_name,
	case
		when data_type = 'datetime' then 'TIMESTAMP'
		when data_type = 'text' then 'STRING'
		when data_type = 'decimal' then 'FLOAT'
		when data_type = 'varchar' then 'VARCHAR'
		when data_type = 'int' then 'INT'
		when data_type = 'bigint' then 'BIGINT'
		when data_type = 'tinyint' then 'INT'
		else '' end as data_type
	FROM information_schema.columns
	WHERE table_schema= 'datax' AND table_name = 'person'
	) a
"
echo "****** JSON *** ONE ***获取 JSON 的 SQL 语句：" 
echo ${create_json}
echo ""

DATA_json="`mysql -h${Y_ip} -p${Y_post} -u${Y_user} -p${Y_password} <<EOF
${create_json}
EOF`"
echo "****** JSON *** TOW ***获取 JSON 的语句：" 
echo ${DATA_json}
echo ""

# 获取字符串长度
length_json=`expr ${#DATA_json} - 9`
# 获取字段属性
createjson=${DATA_json:8:${length_json}}
echo "****** JSON *** THR ***最终 JSON 字符串：" 
echo ${createjson}
echo ""

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
    when column_type like 'int%' then 'INT'
    when column_type like 'decimal%' then 'FLOAT'
    when column_type like 'varchar%' then column_type
    when column_type like 'bigint%' then 'bigint'
		when column_type like 'tinyint%' then 'INT'
		when column_type like 'text%' then 'STRING'
		else '' end as column_type
FROM information_schema.columns
WHERE table_schema= 'datax' AND table_name = 'person'
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
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';
"
echo "****** HIVE *** FOUR ***拼接 HIVE 的建表语句  如果已经纯在，清空表结构：" 
echo ${create_hive}
echo ""

# 远程登录集群，建表
ssh ${M_user}@${M_hive_ip} > /dev/null 2>&1 << eeooff
hive -e "${create_hive}" > /dev/null 2>&1
eeooff
echo "*** HIVE *** FIVE ***表创建成功  OR  或者已经纯在"
echo ""

# 远程连接集群，删除hdfs文件（清空表数据）重新全量的写入
f_cmd="ssh ${M_user}@${M_hdfs_ip} \"${M_hadoop_address}/hadoop fs -rmr ${M_hive_warehouse}/${M_db}.db/${M_table}/* 2>&1\""
echo "####### HDFS ##### 删除hdfs执行语句:"
echo ${f_cmd}
echo ""

if eval ${f_cmd}; then
	echo "========== 删除文件成功========================"
else
	echo "========== 删除文件失败  OR 表内无文件 ========"
fi
echo "###### HDFS ###### 删除 HDFS 上文件成功！！！"
echo ""


# datax执行开始时间
start_tm=$(date  +"%Y-%m-%d %H:%M:%S")
s_tm=`date +%s`

# 运行datax抽数任务
python ./../../bin/datax.py ${M_addr}/ads.json -p "\
-DY_ip='${Y_ip}' -DY_post='${Y_post}' -DY_db='${Y_db}' -DY_table='${Y_table}' -DY_user='${Y_user}' -DY_password='${Y_password}' \
-DM_hive_ip='${M_hive_ip}' -DM_hdfs_ip='${M_hdfs_ip}' -DM_db='${M_db}' -DM_table='${M_table}' -DM_type='${M_type}' \
-DM_json='${createjson}' -DM_hive_warehouse='${M_hive_warehouse}'"

# datax执行结束时间
end_tm=$(date  +"%Y-%m-%d %H:%M:%S")
e_tm=`date +%s`

val=`expr ${e_tm} - ${s_tm}`
echo "DataX执行开始时间: " ${start_tm}
echo "DataX执行结束时间: " ${end_tm}
echo "最终耗时: " ${val} "秒"


