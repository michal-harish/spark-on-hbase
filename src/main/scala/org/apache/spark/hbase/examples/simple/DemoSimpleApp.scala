package org.apache.spark.hbase.examples.simple

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.hbase.Utils

/**
 * Created by mharis on 28/07/15.
 */
class DemoSimpleApp(sc: SparkContext) {

  implicit val context = sc

  Utils.updateSchema(sc, "demo-simple", numRegions = 100, HBaseTableSimple.schema:_*)

  val table = new HBaseTableSimple(sc: SparkContext, "demo-simple")

  def help = {
    println("Spark-on-HBase Graph Demo shell help:")
    println(" help - print this usage information")
    println(" table - main example instance of the HBaseTableSimple `demo-simple`")
    println(" put  - put example data into the underlying hbase table `demo-simple`")
    println(" collect - collect all data from the demo table")
    println(" collectNumCells - count number of cells per row in a specialised HBaseRDD[String, Short]")
    println(" collectFeatures - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Double]]")
    println(" collectPropensity - collect column `propensity` from family `F` as a specialised HBaseRDD[String, Double]")
    println(" collectTags - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Double]]")
    println(" join - example join")
  }

  def put = {
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
    println("Collecting all data from `demo-simple`> table.rdd.collect.foreach(println)")
    table.rdd.collect.foreach(println)
  }

  def collectFeatures = {
    println("> table.rdd.collect.foreach(println)")
    table.rddFeatures.collect.foreach(println)
  }

  def collectPropensity = {
    println("> table.rddFeatures.collect.foreach(println)")
    table.rddFeatures.collect.foreach(println)
  }

  def collectTags = {
    println("> table.rddTags.collect.foreach(println)")
    table.rddTags.collect.foreach(println)
  }
  def collectNumCells = {
    println("> table.rddNumCells.collect.foreach(println)")
    table.rddNumCells.collect.foreach(println)
  }

  def join = {
    println("> table.rddTags.join(table.rddTags.mapValues(_.hashCode)).collect.foreach(println)")
    table.rddTags.join(table.rddTags.mapValues(_.hashCode)).collect.foreach(println)
  }

}
