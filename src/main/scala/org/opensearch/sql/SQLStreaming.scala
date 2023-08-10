package org.opensearch.sql

import org.apache.spark.sql.SparkSession

object SQLStreaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("SQLJob")
      .enableHiveSupport()
      .config("spark.sql.extensions", "org.opensearch.flint.spark.FlintSparkExtensions")
      .getOrCreate()

    var query = """
          CREATE EXTERNAL TABLE http_logs_stream (
           `@timestamp` TIMESTAMP,
           clientip STRING,
           request STRING,
           status INT,
           size INT
          )
          USING json
          OPTIONS (
           path 's3://flint.dev.penghuo.us-west-2/data/http_log/streaming/*',
           compression 'bzip2'
          )
        """
    spark.sql(query)

    spark.sql("show tables").show()

    query = """
          SELECT `@timestamp`, clientip, request, status, size
          FROM http_logs_stream
          LIMIT 64
        """
    val df = spark.sql(query)
    df.show()

    import spark.implicits._  // Import SQL implicits to use $ notation

    // todo
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.OutputMode

    val http_logs_stream = spark.readStream.table("http_logs_stream")

    // Group by 5 minute window and count the number of requests
    val windowedCounts = http_logs_stream
      .groupBy(window($"@timestamp", "5 minutes").as("window"))
      .count()

    val windowedCountsWithId = windowedCounts
      .withColumn("id", unix_timestamp($"window.start", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ").cast("string"))
      .withColumn("window_start", $"window.start")
      .drop("window")

    val streamingQuery = windowedCountsWithId
      .writeStream
      .format("flint")
      .option("checkpointLocation", "s3://flint.dev.penghuo.us-west-2/streaming/checkpointLocation/http_logs_stream")
      .option("write.id_name", "id")
      .outputMode(OutputMode.Complete())
      .start("http_logs_stream")

    streamingQuery.awaitTermination()
  }
}
