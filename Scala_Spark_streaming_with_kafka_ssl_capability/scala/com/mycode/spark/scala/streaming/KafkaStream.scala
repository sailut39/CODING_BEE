package com.mycode.spark.scala.streaming
/**
 * this code is designed using maven project, it helps to stream data from kafka to database directly with a 1 min batch
 * interval, also it uses ssl conncevitity with secure data transfer over network, python has no support for ssl connectivities
 * hence going with scala to acheive it
 * detailed explanation on each and individual steps are available from spark web UI, even we enabled history URL to track back 
 * earlier failed cases as well
 * Run command-
 * export SPARK_MAJOR_VERSION=2;export HADOOP_CONF_DIR=/etc/hadoop/conf/;\
 * spark-submit --master yarn --deploy-mode cluster --queue <ueue_nm> \
 * --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.1 \
 * --files pyspark_sample/streaming/jaas.conf,truststore.jks,<keytab file> \
 * --conf spark.executor.extraJavaOptions="-Djava.security.auth.login.config=jaas.conf" \
 * --conf spark.driver.extraJavaOptions="-Djava.security.auth.login.config=jaas.conf" \
 * --num-executors 2 --executor-cores 1 --executor-memory 1G --driver-memory 1G \
 * --properties-file application.properties \
 * --keytab <keytab file> --principal <key principle> --class com.mycode.spark.scala.streaming.KafkaStream ris_spark_streaming-0.0.1.jar
 * 
 * 
 */


import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.Subscribe
import java.time.LocalDateTime
import java.io.FileInputStream
import scala.collection.JavaConversions._
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import scala.util.parsing.json._
import org.json4s.{ DefaultFormats, MappingException }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }
import java.time.format.{ DateTimeFormatter }
import java.time.{ ZoneId, ZonedDateTime }
import java.util.Properties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.TimestampType
import org.postgresql.util.PSQLException

object KafkaStream {
  def main(args: Array[String]) {  
    val conf = new SparkConf().setAppName("mycode-spark-streaming")
                          .set("spark.driver.allowMultipleContexts", "true")
                          .set("spark.sql.broadcastTimeout", "360000")
                          .set("spark.streaming.backpressure.enabled","true")
                          .set("spark.streaming.kafka.maxRatePerPartition","30000")
                          .set("spark.default.parallelism", "200")
//    val r = new scala.util.Random
    
    
    val sc = new SparkContext(conf);
    val appname = sc.getConf.get("spark.app.name");
    val repartition1 = sc.getConf.get("spark.repartition.tasks");
    val kafkaBootstrapServers = sc.getConf.get("spark.kafka.brokers");
    val kafkaTopicToSubscribe = sc.getConf.get("spark.kafka.topic");
    val kafkaStartingOffsets = sc.getConf.get("spark.kafka.startingOffsets");
    val kafkaStreamProcessingWindow = sc.getConf.get("spark.kafka.stream.window");
    val kafkaSecurityProtocol = sc.getConf.get("spark.kafka.security.protocol");
    val kafkaTrustoreLocation = sc.getConf.get("spark.kafka.ssl.truststore.location");
    val kafkaTrustorePassword = sc.getConf.get("spark.kafka.ssl.truststore.password");
    val kafkaTrustoreType = sc.getConf.get("spark.kafka.ssl.truststore.type");
    val kafkaFailOnDataLoss = sc.getConf.get("spark.kafka.failOnDataLoss");
    val grpid = sc.getConf.get("spark.kafka.grpid");
val ohl_jdbc_url = sc.getConf.get("spark.ohl.url");
val ohl_tgt_tblnm = sc.getConf.get("spark.ohl.tgt_tblname");
val ohl_uname = sc.getConf.get("spark.ohl.uname");
val ohl_passwd = sc.getConf.get("spark.ohl.passwd");
val ohl_jdbc_classname= sc.getConf.get("spark.ohl.jdbc.classnm");

val connectionProperties = new Properties();
connectionProperties.put("user", ohl_uname)
connectionProperties.put("password", ohl_passwd)
connectionProperties.put("stringtype", "unspecified")
connectionProperties.put("driver", ohl_jdbc_classname)

    val ssc = new StreamingContext(sc, Seconds(kafkaStreamProcessingWindow.toInt))
    val spark = SparkSession
      .builder()
      .appName(appname)
      .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version","2")
    spark.conf.set("spark.speculation","false")
    spark.conf.set("spark.streaming.kafka.consumer.cache.maxCapacity","200")

    import spark.implicits._
    import org.apache.spark.sql.types._
    
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaBootstrapServers,
      "group.id" -> grpid,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "kafka.auto.offset.reset" -> kafkaStartingOffsets,
      "security.protocol" -> kafkaSecurityProtocol,
      "ssl.truststore.location" -> kafkaTrustoreLocation,
      "ssl.truststore.password" -> kafkaTrustorePassword,
      "ssl.truststore.type" -> kafkaTrustoreType)
    
val multitopic = kafkaTopicToSubscribe.split(",").toSet

val lines = KafkaUtils.createDirectStream[String, String](
ssc,
PreferConsistent,
Subscribe[String, String](multitopic, kafkaParams))
    
lines.foreachRDD { rdd =>
val data = rdd.map(x => x.value())
//Timezone values with conversions
val pst_fullDateHrMi = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
val pst_fullDateTimeHrsMins = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
val pst_fullDateTimeWithMilli = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS")
val partitions_date=ZonedDateTime.now(ZoneId.of("America/Los_Angeles")).format(pst_fullDateHrMi)
val pst_load_dttm=ZonedDateTime.now(ZoneId.of("America/Los_Angeles")).format(pst_fullDateTimeHrsMins)
val uname=System.getProperty("user.name")

val df1 = data.repartition(200).toDF("json").selectExpr("get_json_object(json,'$.eventId')", "get_json_object(json,'$.eventType')","get_json_object(json,'$.eventVersion')","get_json_object(json,'$.eventTime')","get_json_object(json,'$.eventProducerId')", "json", "get_json_object(json,'$.payload.FileName')").map { row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6)) }

if (df1.count() > 0 ) {
val valuedf = df1.repartition(200).toDF("eventId", "eventType","eventVersion","eventTime","eventProducerId","payload","payloadfilename")

try {
valuedf.repartition(200).withColumn("load_dttm", lit(pst_load_dttm))
.withColumn("created_userid",lit(uname))
.withColumn("update_userid",lit(""))
.withColumn("update_date",lit(null).cast("timestamp"))
.withColumn("deepio_eventid",lit(""))
.select(col("eventId").as("eventid")
, col("eventType").as("eventtype")
, col("eventVersion").as("eventversion")
, coalesce(unix_timestamp($"eventTime", "yyyy-MM-dd HH:mm:ss").cast(TimestampType),unix_timestamp($"eventTime", "yyyy-MM-dd'T'HH:mm:ss").cast(TimestampType)).as("eventtime")
, col("eventProducerId").as("eventproducerid")
, col("payload").as("payload")
, col("payloadfilename").as("payloadfilename")
, col("created_userid").as("created_userid")
, col("load_dttm").as("created_date")
, col("update_userid").as("update_userid")
, col("update_date").as("update_date")
, col("deepio_eventid").as("deepio_eventid")
).write.mode(SaveMode.Append).jdbc(ohl_jdbc_url,ohl_tgt_tblnm,connectionProperties)
} catch {case e: org.postgresql.util.PSQLException => print("PSQL exception happened, but continuing with other RDD \n"+e.printStackTrace+"\n") 
case e: org.apache.spark.SparkException => print("spark exception happened, but continuing with other RDD \n"+e.printStackTrace+"\n")
case e: java.sql.BatchUpdateException => print("Batch exception happened, but continuing with other RDD \n"+e.printStackTrace+"\n")
case e: Throwable => print("unknown exception has been caught!!" + e.printStackTrace+"\n")
}

}
else {
  val dummy=0  //simple logic instead of continue
}
//val storing_loc="s3a://tmo-eds-w2-qat-lake-scm-devops2/inventory/sourcelike/test/"+r.alphanumeric.take(20).mkString("")+".txt"
//valuedf2.select($"payload").write.text(storing_loc)
}
ssc.start()
ssc.awaitTermination()
}
}
