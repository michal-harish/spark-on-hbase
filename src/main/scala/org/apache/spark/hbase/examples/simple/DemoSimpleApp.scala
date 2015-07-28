package org.apache.spark.hbase.examples.simple

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.hbase.Utils

/**
 * Created by mharis on 28/07/15.
 */
class DemoSimpleApp(sc: SparkContext) {

  implicit val context = sc

  object table extends HBaseTableSimple(sc: SparkContext, "demo-simple")

  def help = {
    println("Spark-on-HBase Graph Demo shell help:")
    println(" help - print this usage information")
    println(" create - create (if not exists) the underlying hbase table `demo-simple`")
    println(" update  - update the underlying hbase table `demo-simple`")
    println(" collect - collect all data from the demo table")
    println(" collectNumCells - count number of cells per row in a specialised HBaseRDD[String, Short]")
    println(" collectFeatures - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Double]]")
    println(" collectPropensity - collect column `propensity` from family `F` as a specialised HBaseRDD[String, Double]")
    println(" collectTags - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Double]]")
  }

  def create = {
    Utils.updateSchema(sc, "demo-simple", numRegions = 100, HBaseTableSimple.schema:_*)
  }

  def update = {
    println("Updating `demo-simple` column family `F` - Features")
    val dataForColumnFamilyF = sc.parallelize(Array(
      "row1" -> Map(
        Bytes.toBytes("propensity") ->(Bytes.toBytes(0.5), System.currentTimeMillis),
        Bytes.toBytes("width") ->(Bytes.toBytes(100.0), System.currentTimeMillis)
      ),
      "row2" -> Map(
        Bytes.toBytes("propensity") ->(Bytes.toBytes(0.9), System.currentTimeMillis),
        Bytes.toBytes("width") ->(Bytes.toBytes(300.0), System.currentTimeMillis)
      )
    ))
    table.update(Bytes.toBytes("F"), dataForColumnFamilyF)

    println("Updating `demo-simple` column family `T` - Tags")
    val dataForColumnFamilyT = sc.parallelize(Array(
      "row1" -> Map(
        Bytes.toBytes("lego") ->(Array[Byte](), System.currentTimeMillis),
        Bytes.toBytes("music") ->(Array[Byte](), System.currentTimeMillis),
        Bytes.toBytes("motorbike") ->(Array[Byte](), System.currentTimeMillis)
      ),
      "row2" -> Map(
        Bytes.toBytes("cinema") ->(Array[Byte](), System.currentTimeMillis)
      )
    ))
    table.update(Bytes.toBytes("T"), dataForColumnFamilyT)
  }

  def collect = {
    println("Collecting all data from `demo-simple`")
    table.rdd.collect.foreach(println)
  }

  def collectFeatures = {
    table.rddFeatures.collect.foreach(println)
  }

  def collectPropensity = {
    table.rddFeatures.collect.foreach(println)
  }

  def collectTags = {
    table.rddTags.collect.foreach(println)
  }
  def collectNumCells = {
    table.rddNumCells.collect.foreach(println)
  }

}
