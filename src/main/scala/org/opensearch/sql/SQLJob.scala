package org.opensearch.sql

import org.apache.spark.sql.SparkSession

object SQLJob {
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
    spark.sql(query).show()

    spark.sql("show tables").show()

    query = """
          SELECT `@timestamp`, clientip, request, status, size
          FROM http_logs_stream
          LIMIT 64
          """
    val df = spark.sql(query)
    df.show()

    val aos = Map("host" -> "search-test25-znvgc3dcs5plrnmq6otiyfsppy.us-west-2.es.amazonaws.com", "port" -> "-1", "scheme" -> "https", "auth" -> "sigv4", "region" -> "us-west-2")
    val res = df.write.format("flint").options(aos).mode("overwrite").save("http_logs_batch")
  }
}
