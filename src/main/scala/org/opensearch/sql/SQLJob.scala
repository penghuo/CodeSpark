package org.opensearch.sql

import org.apache.spark.sql.functions.{avg, count, window}
import org.apache.spark.sql.{SparkSession}

object SQLJob {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .config(
        "spark.hadoop.hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
      )
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._  // Import SQL implicits to use $ notation

    spark.readStream.table("default.alb_logs_temp")
      .withWatermark("time", "10 seconds")
      .groupBy(window($"time", "1 minute"))
      .agg(count("time").alias("request_count"), avg("request_processing_time").alias("Latency_in_seconds"))
      .writeStream
      .queryName("alb_logs_metrics")
      .format("parquet")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/alb_logs_metrics")
      .option("path", "s3a://flint.dev.penghuo.us-west-2/alb_logs_metrics/")
      .start()
      .awaitTermination()
  }
}
