package org.opensearch.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration._

object SQLJob extends Logging {
  // // Define a function to recursively unpersist RDD and its dependencies
  // def unpersistRecursive(rdd: RDD[_]): Unit = {
  //   // Unpersist the current RDD
  //   logInfo(s">> before unpersist")
  //   rdd.unpersist(true)
  //   logInfo(s">> after unpersist")

  //   logInfo(s">> before cleanShuffleDependencies")
  //   rdd.cleanShuffleDependencies(true)
  //   logInfo(s"<< after cleanShuffleDependencies")
    
  //   // Recursively unpersist each dependency RDD
  //   rdd.dependencies.foreach { dep =>
  //     val depRDD = dep.rdd
  //     unpersistRecursive(depRDD)
  //   }
  // }

  def main(args: Array[String]) {
    val query =  args(0)
    val clearShuffle = args(1).toBoolean

    // create spark session
    val spark = SparkSession.builder()
      .config(
        "spark.hadoop.hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
      )
      .enableHiveSupport()
      .getOrCreate()

    // Execute each SQL query with a 10-second delay in between
    logInfo(s"Executing query: $query")
    val startTime = System.nanoTime()
    
    // Execute SQL query
    var result = spark.sql(query)
    
    // Show the result of the query
    result.show()

    if (clearShuffle) {
//     logInfo(s">> before cleanShuffleDependencies")
//     result.rdd.cleanShuffleDependencies(true)
//     logInfo(s"<< after cleanShuffleDependencies")

      logInfo("Waiting for 1 mins...")
      Thread.sleep(60.seconds.toMillis)

//     logInfo(s">> before unpersist")
//     result.rdd.unpersist(true)
//     logInfo(s">> after unpersist")

      logInfo(s">> Before gc")
      System.gc()
      logInfo(s"<< After gc")
    }

    // Calculate and print the execution time
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    println(s"Query executed in $duration seconds")

    logInfo("Waiting for 5 mins...")
    Thread.sleep(300.seconds.toMillis)

    // Stop the SparkSession
    spark.stop()    
  }
}
