package org.opensearch.sql

import org.apache.spark.sql.SparkSession

object SQLJob {
  def main(args: Array[String]) {
    // Get the SQL query from the command line arguments
    val sql = args(0)

    // create spark session
    val spark = SparkSession.builder()
      .config(
        "spark.hadoop.hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
      )
      .enableHiveSupport()
      .getOrCreate()

    // Execute the SQL query
    val df = spark.sql(sql)

    // Show the results
    df.show()

//    // Stop the SparkSession
//    spark.stop()
    spark.streams.awaitAnyTermination()
  }
}
