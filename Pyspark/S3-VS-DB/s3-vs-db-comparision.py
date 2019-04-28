#!/usr/bin/python
'''
Created on Aug 21, 2018
@author: STata2

Run command-
export SPARK_MAJOR_VERSION=2;export HADOOP_CONF_DIR=<hadoop_loc>/conf/;spark-submit --master yarn --deploy-mode cluster --jars <path_jar>/postgresql-42.2.2.jar --queue <yarn_queue> --keytab <keytabfile_location> --principal <key_tab_principal> --num-executors 2 --executor-cores 2 --executor-memory 1G --driver-memory 1G --properties-file <path_for_properties>/application_counts.properties <pymodule_loc>/v2.py
Brief about this program:
-    This program helps to match ref values between s3 data and database table with in the given time boundaries 
     and it will capture result of only missed data on either of systems
-    Most of the high level parameterization have been taken care from properties file
How it works:
-    Based on provided message names, process will compare data between s3 folder and DB tables
-    as per provided time boundaries spark process will bring data from s3 for all the dates
- Always pg query will limit data based on >=(mysql_partition_val) and < (prepared end val -> which is start val for next job) condition
Assumptions and facts:
1.    Everyday one directory is expected on S3 folder otherwise spark job will fail with analysis exception
2.    All dates are adhere to PST timezones 

'''
import traceback
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import ConfigParser,getpass,sys,os
import datetime
from datetime import date, timedelta
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

os.environ["TZ"]='US/Pacific'

spark = SparkSession.builder.appName('S3-Vs-DB-data-validator').enableHiveSupport().getOrCreate()

cx = spark.sparkContext.getConf()
user_name=spark.sparkContext.sparkUser()
dbtype = cx.get("spark.dbtype")
host_nm = cx.get("spark.host_nm")
port = cx.get("spark.port")
db_name = cx.get("spark.db_name")
pg_user_nm = cx.get("spark.pg_user_nm")
jceks_db_passwd = cx.get("spark.jceks_db_passwd")
inp_loc = cx.get("spark.inp_loc")
capture_log_table = cx.get("spark.capture_log_table")
postgres_src_tbl = cx.get("spark.postgres_src_tbl")
eventtype_list=eval(cx.get("spark.msg_list"))
eventtype_set=set(eventtype_list)

# Prepare list of src_like dates to pick up
pg_mysql_lkp_start_dttm = cx.get("spark.my_sql_part_start_val")
pg_end_boundary_value = cx.get("spark.my_sql_part_end_val")
src_like_final_val=cx.get("spark.src_like_final_val")
tgt_errfile_val=cx.get("spark.db_errfile_val")
custom_partition_value=int(cx.get("spark.custom.partition.value"))

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",10485760)
spark.conf.set("spark.sql.shuffle.partitions",custom_partition_value)
src_like_part_list=src_like_final_val.split(",")
tgt_err_file_part_list=tgt_errfile_val.split(",")

url = 'jdbc:' + dbtype + '://' + host_nm + ':' + port + '/' + db_name

#Decrypt jceks file for password
x1 = spark.sparkContext._jsc.hadoopConfiguration()
x1.set('hadoop.security.credential.provider.path', jceks_db_passwd)
a1 = x1.getPassword(pg_user_nm)
db_passwd = ''
for i in range(a1.__len__()):
    db_passwd = db_passwd + str(a1.__getitem__(i))

prop = eval(cx.get("spark.prop"))
postgres_src_tbl_qry="(select <required_cols>,(created_date AT TIME ZONE 'US/Pacific') as created_date,<required_cols> from "+postgres_src_tbl+" where (created_date AT TIME ZONE 'US/Pacific') >= to_timestamp('"+pg_mysql_lkp_start_dttm+"','yyyyMMddHH24MI') - interval '1' day  and (created_date AT TIME ZONE 'US/Pacific') < to_timestamp('"+pg_end_boundary_value+"','yyyyMMddHH24MI') + interval '1' day ) as t"
postgres_tgt_tbl_qry="(select <required_cols> from "+capture_log_table+" where (etl_created_date AT TIME ZONE 'US/Pacific') >= to_timestamp('"+pg_mysql_lkp_start_dttm+"','yyyyMMddHH24MI') - interval '1' day and (etl_created_date AT TIME ZONE 'US/Pacific') < to_timestamp('"+pg_end_boundary_value+"','yyyyMMddHH24MI') + interval '1' day ) as t"

df_deepio_json = spark.read.format("json").load(src_like_part_list).withColumn("src_like_filename",input_file_name()).where(col("eventType").isin(eventtype_set)).select(<required_cols>).coalesce(custom_partition_value).cache()

print("@@@@@@@@@ "+postgres_src_tbl_qry)
#Get data from <ref_table>
df_jdbc_ref_tbl=spark.read.jdbc(url, postgres_src_tbl_qry, properties=prop)
# following data frame is to verify the existing data on capture table, it will help to eliminate dups
df_jdbc_tgt_tbl=spark.read.jdbc(url, postgres_tgt_tbl_qry, properties=prop)

try:
    df_deepio_json.join(df_jdbc_ref_tbl,(df_jdbc_ref_tbl.<col1> == df_deepio_json.<col1>) & (df_jdbc_ref_tbl.<col2> == df_deepio_json.<col2>),how="left")\
    .filter(df_jdbc_ref_tbl.<col3>.isNull())\
    .join(df_jdbc_tgt_tbl,(df_deepio_json.<col> == df_jdbc_tgt_tbl.<col>) & (df_deepio_json.<col> == df_jdbc_tgt_tbl.<col>),how="left")\
    .filter(df_jdbc_tgt_tbl.<col>.isNull()) \
    .select(df_deepio_json.<col>.alias("<alias_col>")\
    ,df_deepio_json.<col>.alias("<alias_col>")\
    ,df_deepio_json.<col>.alias("<alias_col>")\
    ,df_deepio_json.<col>.alias("<alias_col>")\
    ,df_deepio_json.<col>.alias("<alias_col>")\
    ,df_deepio_json.<col>.alias("<alias_col>")\
    ,df_jdbc_ref_tbl.<col>.alias("<alias_col>")\
    ,df_jdbc_ref_tbl.<col>.alias("<alias_col>")\
    ,df_jdbc_ref_tbl.<col>.alias("<alias_col>")\
    ,df_jdbc_ref_tbl.<col>.alias("<alias_col>")\
    ,lit("").alias("<alias_col>")\
    ,df_jdbc_ref_tbl.<col>.alias("<alias_col>")\
    ,when(df_deepio_json.<col>.isNull() ,'missed on S3-Hive')\
    .when(df_jdbc_ref_tbl.<col>.isNull() ,'missed on DB').alias("observation") \
    ,lit(user_name).alias("created_userid")\
    ,lit(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')).alias("etl_created_date")\
    ).write.jdbc(url,capture_log_table,"append",properties=prop)

except Exception, e:
    print ('exception occured during DB load for '+capture_log_table)
    print(traceback.format_exc())

df_deepio_json.unpersist()
df_jdbc_ref_tbl.unpersist()
df_jdbc_tgt_tbl.unpersist()
spark.stop()