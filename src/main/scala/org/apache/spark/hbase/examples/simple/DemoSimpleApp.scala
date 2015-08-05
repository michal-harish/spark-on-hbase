package org.apache.spark.hbase.examples.simple

import org.apache.spark.SparkContext
import org.apache.spark.hbase.Utils

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
    println(" collectFeatures - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Double]]")
    println(" collectPropensity - collect column `propensity` from family `F` as a specialised HBaseRDD[String, Double]")
    println(" collectTags - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Double]]")
    println(" collectTagsAndFeatures - collect both column families T and F from the demo table and map it to specialised HBaseRDD[String, (List[String], Map[String,Double])]")
    println(" join - example join")
    println(" rightOuterJoin - example rightOuterJoin")
  }

  def update = {
    println("Updating `demo-simple` column family `F` - Features")

    table.update(table.Features, sc.parallelize(Array(
      "row1" -> Map("propensity" -> 0.5, "width" -> 100.0),
      "row2" -> Map("propensity" -> 0.9, "width" -> 300.0)
    )))

    println("Updating `demo-simple` column family `T` - Tags")
    table.update(table.Tags, sc.parallelize(Array(
      "row1" -> List("lego", "music", "motorbike"),
      "row2" -> List("cinema")
    )))

    println("Updating `demo-simple` column `F:propensity` - Tags")
    table.update(table.Propensity, sc.parallelize(Array(
      "row1" -> 0.55,
      "row2" -> 0.95,
      "row3" -> 0.99
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

  def collectPropensity = {
    println("> table.select(table.Propensity).collect.foreach(println)")
    table.select(table.Propensity).collect.foreach(println)
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

}
