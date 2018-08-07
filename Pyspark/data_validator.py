#
# This program will create dataframe from existing file system either S3/HDFS
# Assuming all data is in JDBC format
# Agenda-
#    1- Reconcile data between received Json messages ( 
#        4 kinds of information about messages, 
#            1. direct message from upstream, expecting these are already available on file system 
#            2. a notification message with information about downloaded message, this process will skip download stuff assuming file is already available, 
#            3. Information created when 2 kind of message is failed to download)
#            4. All the upstream details are available on another RDBMS table as second reference for reconcilation 
#    Idea is to log the messages which are not available on receiver but got notified from upstream, there could be several reasons to loose a message,
#   I gonna capture this information on another RDBMS table along with a error reason, this process will ensure no dups created on target log table
# How I am going to do this??
#        Get Json DFs on top of 3 kind of files or messages
#        Get existing target data and perform a join with DFs to pull out report to final system
#Quick observation-
#        This program should run on only spark2.0+ version, because JDBC Dataframe will get schema from RDMBS and it will not change even on cascaded dataframes, so not null
#        will always a not null in entire program, it is a kind of bug in older versions of spark
# Execution command-
#/usr/hdp/2.6.2.0-205/spark2/bin/spark-submit --jars /home/stata2/jars/postgresql-42.2.2.jar \
#--master <local/yarn/yarn-client> \
#--queue spark_streaming \
#--num-executors 10 \
#--driver-memory 2g \
#--executor-memory 2g \
#--executor-cores 2 \
#./pyspark_sample/data_pumper.py \
#./pyspark_sample/tgt_postgres.ini


from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import ConfigParser,datetime,getpass,sys

sc = SparkContext(appName = "data_validator")
sqlContext = SQLContext(sc)
#today_dt=datetime.datetime.today().strftime('%Y%m%d')
cfg = ConfigParser.ConfigParser()
cfg.read(sys.argv[1])
dbtype=cfg.get('mydb','dbtype')
host_nm=cfg.get('mydb','host_nm')
port=cfg.get('mydb','port')
db_name=cfg.get('mydb','db_name')
url = "jdbc:"+dbtype+"://"+host_nm+":"+port+"/"+db_name
prop=eval(cfg.get('mydb','prop'))
capture_log_table=cfg.get('mydb','capture_log_table')
inp_loc=cfg.get('mydb','inp_loc')
postgres_src_tbl_qry="(select eventid,eventtype,eventtime,created_date,upstream_eventid from "+cfg.get('mydb','mydb_src_tbl')+" ) as t"
capture_log_table_qry="(select eventid,eventtype,created_date from "+capture_log_table+" ) as t"

payload_dwnld_loc=cfg.get('postgres','payload_dwnld_loc')+today_dt
error_file_loc=cfg.get('postgres','error_file_loc')+today_dt

# Get all files into DF
df_deepio_json = sqlContext.read.format("json").load(inp_loc).withColumn("src_like_filename",input_file_name())

df_payloaddownload_json=sqlContext.read.format("json").load(payload_dwnld_loc).withColumn("download_filename_alone",reverse(split(reverse(input_file_name()),'\/')[0])).withColumn("src_like_filename",input_file_name())

df_error_files=sqlContext.read.format("text").load(error_file_loc).\
select(reverse(split(reverse(input_file_name()),'\/')[0]).alias("err_filename"),\
split(reverse(split(reverse(input_file_name()),'\/')[0]),'\.')[0].alias("err_eventId")\
,input_file_name().alias("path_err_file"))

df_jdbc_eventmaster=sqlContext.read.jdbc(url, postgres_src_tbl_qry, properties=prop)
df_jdbc_exist_tgt=sqlContext.read.jdbc(url,capture_log_table_qry, properties=prop)

df_final= df_deepio_json.join(df_payloaddownload_json,df_deepio_json.payload.FileName == df_payloaddownload_json.download_filename_alone,how="left_outer")\
.join(df_error_files,(df_error_files.err_eventId == df_deepio_json.eventId) & (df_deepio_json.payload.FileName.isNotNull()),how="left_outer")\
.join(df_jdbc_eventmaster,df_jdbc_eventmaster.upstream_eventid == df_deepio_json.eventId,how="left_outer")\
.join(df_jdbc_exist_tgt, df_jdbc_exist_tgt.eventid == df_deepio_json.eventId,how="left_outer" )\
.filter(df_jdbc_eventmaster.eventid.isNull() & df_jdbc_exist_tgt.eventid.isNull())\
.select(df_deepio_json.eventId.alias("eventid")\
,df_deepio_json.eventType.alias("eventtype")\
,df_deepio_json.eventVersion.alias("eventversion")\
,df_deepio_json.eventTime.alias("eventtime")\
,df_deepio_json.eventProducerId.alias("eventproducerid")\
,df_deepio_json.payload.FileName.alias("deep_payload_filename")\
,df_deepio_json.src_like_filename.alias("deep_src_like")\
,df_payloaddownload_json.eventId.alias("dwnld_eventid")\
,df_payloaddownload_json.eventType.alias("dwnld_eventtype")\
,df_payloaddownload_json.eventVersion.alias("dwnld_eventversion")\
,df_payloaddownload_json.eventTime.alias("dwnld_eventtime")\
,df_payloaddownload_json.eventProducerId.alias("dwnld_eventproducerid")\
,df_payloaddownload_json.src_like_filename.alias("dwnld_src_like")\
,df_error_files.path_err_file.alias("err_file_locn")\
,when(df_deepio_json.payload.FileName.isNotNull() & df_error_files.err_eventId.isNotNull() & df_payloaddownload_json.eventId.isNull()\
        ,'error_file found and no payload file downloaded')\
    .when(df_deepio_json.payload.FileName.isNotNull() & df_error_files.err_eventId.isNotNull()\
        ,'error_file found')\
    .when(df_deepio_json.payload.FileName.isNotNull() & df_payloaddownload_json.eventId.isNull()\
        ,'payload download file not found').alias('error_message')\
,lit(getpass.getuser()).alias("created_userid")\
,lit(datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')).alias("created_date")\
,lit('Not loaded').alias("tgt_load_status")\
).write.jdbc(url,capture_log_table,"append",properties=prop)
