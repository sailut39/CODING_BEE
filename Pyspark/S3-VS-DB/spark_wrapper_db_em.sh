#!/bin/ksh/
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#Description- High level wrapper script to call actual control with partition vals
#             
#Usage-
# <script_nm> 
# example: sh s3_stg_vs_DB_stg.sh 
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
export TZ=America/Los_Angeles
# Mysql entries sake-
export jobName_start="<my_sql_entry_for_timestamp_pull>"
export jobName_last="<<my_sql_previous_entry>"
export src_like_loc="s3a://<S3_bucket_nm>/<path_name>/"
export ods_error_file_loc="s3a://<S3_bucket_nm>/<path_name>/"
export ods_error_file_dummy_loc="s3a://<S3_bucket_nm>/<path_name>/load_dt=19000000"

export my_sql_part_start_val=$(<mysql_partition_extraction>)

if [ "$my_sql_part_start_val" == "No Partition Found. Please insert record in MySql table" ]; then
	echo "mysql partition value doesnt exist!!"
	exit 9
fi

export my_sql_part_end_val=$(date +"%Y%m%d2359")
if [ "${my_sql_part_start_val:-999912312359}" -ge "${my_sql_part_end_val:-1}" ]; then
	echo "Please run this process after sometime, appears dates are not aligned!!"
	echo "my_sql_part_start_val - ${my_sql_part_start_val}"
	echo "my_sql_part_end_val - ${my_sql_part_end_val}"
	exit 1
fi

export start_date=$(echo ${my_sql_part_start_val} | cut -b 1-8)
export end_date=$(echo ${my_sql_part_end_val} | cut -b 1-8)

echo "######################${my_sql_part_start_val}"
echo "######################${my_sql_part_end_val}"
echo "@@@@@@@@@@@ ${start_date} "
echo "@@@@@@@@@@@ ${end_date} "

export src_like_final_val=""
export tgt_errfile_val=""
export src_like_temp_val=""
export ods_errorfile_temp_val=""

export d=
export n=0
until [ "$d" = "$end_date" ]
do
    d=$(date -d "$start_date + $n days" +%Y%m%d)
	export src_like_dum=${src_like_loc}"load_date="$d
	export ods_err_dum=${ods_error_file_loc}"load_dt="$d
	echo "-----------> source_like dum is ${src_like_dum} "
	
	hdfs dfs -test -d $src_like_dum
	[[ $? -eq 0 ]] && src_like_temp_val=${src_like_dum}","${src_like_temp_val}
#	hdfs dfs -test -d $ods_err_dum
#	[[ $? -eq 0 ]] && ods_errorfile_temp_val=${ods_err_dum}","${ods_errorfile_temp_val}
	((n++))
done

src_like_final_val=$(echo ${src_like_temp_val} | sed "s/,$//g")
tgt_errfile_val=$(echo ${ods_errorfile_temp_val} | sed "s/,$//g")


echo "src like available dirs in given time boundaries are "${src_like_final_val}
echo "Target error dirs in given time boundaries are "${tgt_errfile_val}

[[ -z ${src_like_final_val} ]] && echo "None of the srclike files were picked, hence existing!!" && exit -4

echo "about to run Spark command@@@@@@@@@@@@@@"
export SPARK_MAJOR_VERSION=2;export HADOOP_CONF_DIR=/etc/hadoop/conf/;spark-submit \
--master yarn \
--deploy-mode cluster \
--jars /efs/eit_hadoop/applications/retail_inventry_ingest/lib/postgresql-42.2.2.jar \
--keytab <keytab_file_location> \
--queue <yarn_que_name> \
--principal <key_tab_dtls> \
--num-executors 10 \
--executor-cores 5 \
--executor-memory 10G \
--driver-memory 30G \
--conf spark.my_sql_part_start_val=${my_sql_part_start_val} \
--conf spark.my_sql_part_end_val=${my_sql_part_end_val} \
--conf spark.src_like_final_val=${src_like_final_val} \
--conf spark.tgt_errfile_val=${tgt_errfile_val:-${ods_error_file_dummy_loc}} \
--conf spark.locality.wait=100s \
--conf spark.default.parallelism=16 \
--conf spark.executor.heartbeatInterval=360s \
--conf spark.network.timeout=600s \
--conf spark.driver.maxResultSize=40g \
--conf spark.yarn.executor.memoryOverhead=20G \
--conf spark.yarn.driver.memoryOverhead=4096 \
--conf spark.port.maxRetries=20 \
--conf spark.dynamicAllocation.initialExecutors=2 \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf spark.sql.session.timeZone="US/Pacific" \
--properties-file application_src_like-vs-tgt_em.properties \
s3-vs-db-comparision.py

if [ $? == 0 ]; then
	export my_sql_part_end_val=$(date +"%Y%m%d%H%M")
	<logic_to_update_partition_on_mysql>
	if [ $? != 0 ]; then
		echo "failed to update mysql partition for next run!! please update the value manually for next run!!"
		echo "I tried to update the partition with value- ${my_sql_part_end_val}"
		exit -1
	fi
	echo "I have updated the value for next run -value is -> ${my_sql_part_end_val}"
	<logic_to_update_last_partition_val_on_mysql>
	echo "###############@@@@@${update1}########"
	if [ $? != 0 ]; then
		echo "failed to update mysql partition for last partition value!! please update the value manually for next run!!"
		echo "I tried to update the last partition with value- ${my_sql_part_start_val}"
		exit -2
	fi
echo "process has successfully completed!!"
else
echo "Error occurred with spark submit job!!"
exit -3
fi
