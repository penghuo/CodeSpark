package org.opensearch.sql

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SQLJob {
  def main(args: Array[String]) {
    // Get the SQL query from the command line arguments
    val sql = args(0)

    // Create a SparkSession
    val spark = SparkSession.builder().appName("SQLJob").getOrCreate()

    // Execute the SQL query
    val df: Dataset[Row] = spark.sql(sql)

    // Show the results
    df.show()

    // Stop the SparkSession
    spark.stop()
  }
}
