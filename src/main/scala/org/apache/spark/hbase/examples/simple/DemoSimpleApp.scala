package org.apache.spark.hbase.examples.simple

import org.apache.spark.SparkContext
import org.apache.spark.hbase.Utils
import org.apache.spark.hbase.helpers.TLong

/**
 * Created by mharis on 28/07/15.
 *
 */
class DemoSimpleApp(sc: SparkContext) {

  implicit val context = sc

  Utils.updateSchema(sc, "demo-simple", numRegions = 32, HBaseTableSimple.schema: _*)

  val table = new HBaseTableSimple(sc: SparkContext, "demo-simple")

  def help = {
    println("Spark-on-HBase Simple Demo shell help:")
    println(" help - print this usage information")
    println(" table - main example instance of the HBaseTableSimple `demo-simple`")
    println(" collect <RDD> - collet and print the given rdd fully")
    println(" update  - put example data into the underlying hbase table `demo-simple` using Transformation")
    println(" collect - collect all data from the demo table")
    println(" collectNumCells - count number of cells per row in a specialised HBaseRDD[String, Short]")
    println(" collectFeatures - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Long]]")
    println(" collectHeight - collect column `height` from family `F` as a specialised HBaseRDD[String, Long]")
    println(" collectTags - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Double]]")
    println(" collectTagsAndFeatures - collect both column families T and F from the demo table and map it to specialised HBaseRDD[String, (List[String], Map[String,Double])]")
    println(" join - example join")
    println(" rightOuterJoin - example rightOuterJoin")
    println(" filter - example transformation filter")
  }

  def update = {
    println("Updating `demo-simple` column family `F` - Features")

    table.update(table.Features, sc.parallelize(Array(
      "row1" -> Map("height" -> 50L, "width" -> 100L),
      "row2" -> Map("height" -> 90L, "width" -> 300L)
    )))

    println("Updating `demo-simple` column family `T` - Tags")
    table.update(table.Tags, sc.parallelize(Array(
      "row1" -> List("lego", "music", "motorbike"),
      "row2" -> List("cinema")
    )))

    println("Updating `demo-simple` column `F:height`")
    table.update(TLong("F:height"), sc.parallelize(Array(
      "row1" -> 55L,
      "row2" -> 95L,
      "row3" -> 99L
    )))
  }

  def collect = {
    println("Collecting all raw data from `demo-simple`> table.rdd.collect.foreach(println)")
    table.rdd.collect.foreach(println)
  }

  def collectFeatures = {
    println("> table.select(table.Features).collect.foreach(println)")
    table.select(table.Features).collect.foreach(println)
  }

  def collectHeight = {
    println("> table.select(TLong(\"F:height\")).collect.foreach(println)")
    table.select(TLong("F:height")).collect.foreach(println)
  }

  def collectTags = {
    println("> table.select(table.Tags).collect.foreach(println)")
    table.select(table.Tags).collect.foreach(println)
  }

  def collectNumCells = {
    println("> table.select(table.CellCount).collect.foreach(println)")
    table.select(table.CellCount).collect.foreach(println)
  }

  def collectTagsAndFeatures = {
    println("> table.select(table.Tags, table.Features).collect.foreach(println)")
    table.select(table.Tags, table.Features).collect.foreach(println)
  }

  def join = {
    println("> val other = sc.parallelize(Array(\"row1\" -> \"Moo\", \"row2\" -> \"Foo\", \"row3\" -> \"Bar\"))")
    println("> table.select(table.Tags).join(other).collect.foreach(println)")
    val other = sc.parallelize(Array("row1" -> "Moo", "row2" -> "Foo", "row3" -> "Bar"))
    table.select(table.Tags).join(other).collect.foreach(println)
  }

  def rightOuterJoin = {
    println("> val other = sc.parallelize(Array(\"row1\" -> \"Moo\", \"row2\" -> \"Foo\", \"row3\" -> \"Bar\"))")
    val other = sc.parallelize(Array("row1" -> "Moo", "row2" -> "Foo", "row3" -> "Bar"))
    println("> table.select(table.Tags).rightOuterJoin(other).collect.foreach(println)")
    table.select(table.Tags).rightOuterJoin(other).collect.foreach(println)
  }

  def filter = {
    println("> table.select(table.Features).filter(table.Tags contains \"lego\").collect.foreach(println)")
    table.select(table.Features).filter(table.Tags contains "lego").collect.foreach(println)
  }
}
