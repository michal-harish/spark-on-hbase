package org.apache.spark.hbase.examples.simple

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.spark.SparkContext
import org.apache.spark.hbase.{Utils, HBaseTable}

/**
 * Created by mharis on 28/07/15.
 */
class DemoSimpleApp(sc: SparkContext) {

  implicit val context = sc

  object table extends HBaseTableSimple(sc: SparkContext, "demo-simple")

  def help = {
    println("Spark-on-HBase Graph Demo shell help:")
    println(" help - print this usage information")
    println(" table - default demo instance of a table")
    println(" create - create (if not exists) the underlying hbase table `demo-simple`")
    println(" update  - update the underlying hbase table `demo-simple`")
    println(" collect - collect all data from the demo table")
  }

  def create = {
    println("Creating table `demo-simple` with 2 column families: H, F")
    Utils.createIfNotExists(sc, "demo-simple", numRegions = 100,
      Utils.column("H", inMemory = false, ttlSeconds = 86400 * 90, BloomType.ROW, maxVersions = 1, Algorithm.SNAPPY, blocksize = 64 * 1024),
      Utils.column("F", inMemory = false, ttlSeconds = 86400 * 90, BloomType.ROWCOL, maxVersions = 1, Algorithm.SNAPPY, blocksize = 64 * 1024)
    )
  }

  def update = {
    println("Updating `demo-simple` column family F")
    val dataToInsert = sc.parallelize(Array(
      "row1" -> Map(
        Bytes.toBytes("col1") ->(Bytes.toBytes("value1"), System.currentTimeMillis),
        Bytes.toBytes("col2") ->(Bytes.toBytes("value1"), System.currentTimeMillis)
      ),
      "row2" -> Map(
        Bytes.toBytes("col1") ->(Bytes.toBytes("value2"), System.currentTimeMillis),
        Bytes.toBytes("col2") ->(Bytes.toBytes("value1"), System.currentTimeMillis)
      )
    ))
    table.update(Bytes.toBytes("F"), dataToInsert)

    //println("Updating `demo-simple` column family H")
  }

  def collect = {
    println("Collecting all data from `demo-simple`")
    table.rdd.collect.foreach(println)
  }

}
