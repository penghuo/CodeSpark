package org.opensearch.sql

import org.apache.spark.sql.SparkSession
import org.opensearch.spark.sql._


object OpenSearchRW {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .config(
        "spark.hadoop.hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
      )
      .enableHiveSupport()
      .getOrCreate()  // Import SQL implicits to use $ notation

    val domain = args(0)
    val sql = spark.sqlContext
    val accountsRead = sql.read.json("s3a://flint.dev.penghuo.us-west-2/data/accounts.json")
    val options = Map("pushdown" -> "true",
      "opensearch.nodes" -> domain,
      "opensearch.aws.sigv4.enabled" -> "true",
      "opensearch.aws.sigv4.region" -> "us-west-2",
      "opensearch.nodes.resolve.hostname" -> "false",
      "opensearch.nodes.wan.only" -> "true",
      "opensearch.net.ssl" -> "true")
    accountsRead.saveToOpenSearch("accounts", options)
  }
}
