package org.apache.spark.hbase.examples.simple

import org.apache.spark.SparkContext
import org.apache.spark.hbase.Utils
import org.apache.spark.hbase.examples.simple.HBaseTableSimple._

/**
 * Created by mharis on 28/07/15.
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
    println(" put  - put example data into the underlying hbase table `demo-simple`")
    println(" collect - collect all data from the demo table")
    println(" collectNumCells - count number of cells per row in a specialised HBaseRDD[String, Short]")
    println(" collectFeatures - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Double]]")
    println(" collectPropensity - collect column `propensity` from family `F` as a specialised HBaseRDD[String, Double]")
    println(" collectTags - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Double]]")
    println(" collectTagsAndFeatures - collect both column families T and F from the demo table and map it to specialised HBaseRDD[String, (List[String], Map[String,Double])]")
    println(" join - example join")
    println(" rightOuterJoin - example rightOuterJoin")
  }

  def put = {
    println("Updating `demo-simple` column family `F` - Features")

    table.update(Features, sc.parallelize(Array(
      "row1" -> Map("propensity" -> 0.5, "width" -> 100.0),
      "row2" -> Map("propensity" -> 0.9, "width" -> 300.0)
    )))

    println("Updating `demo-simple` column family `T` - Tags")
    table.update(Tags, sc.parallelize(Array(
      "row1" -> List("lego", "music", "motorbike"),
      "row2" -> List("cinema")
    )))
  }

  def collect = {
    println("Collecting all raw data from `demo-simple`> table.rdd.collect.foreach(println)")
    table.rdd.collect.foreach(println)
  }

  def collectFeatures = {
    println("> table.select(Features).collect.foreach(println)")
    table.select(Features).collect.foreach(println)
  }

  def collectPropensity = {
    println("> table.select(Propensity).collect.foreach(println)")
    table.select(Propensity).collect.foreach(println)
  }

  def collectTags = {
    println("> table.select(Tags).collect.foreach(println)")
    table.select(Tags).collect.foreach(println)
  }

  def collectNumCells = {
    println("> table.select(CellCount).collect.foreach(println)")
    table.select(CellCount).collect.foreach(println)
  }

  def collectTagsAndFeatures = {
    println("> table.select(Tags, Features).collect.foreach(println)")
    table.select(Tags, Features).collect.foreach(println)
  }

  def join = {
    println("> val other = sc.parallelize(Array(\"row1\" -> \"Moo\", \"row2\" -> \"Foo\", \"row3\" -> \"Bar\"))")
    println("> table.select(Tags).join(other).collect.foreach(println)")
    val other = sc.parallelize(Array("row1" -> "Moo", "row2" -> "Foo", "row3" -> "Bar"))
    table.select(Tags).join(other).collect.foreach(println)
  }

  def rightOuterJoin = {
    println("> val other = sc.parallelize(Array(\"row1\" -> \"Moo\", \"row2\" -> \"Foo\", \"row3\" -> \"Bar\"))")
    val other = sc.parallelize(Array("row1" -> "Moo", "row2" -> "Foo", "row3" -> "Bar"))
    println("> table.select(Tags).rightOuterJoin(other).collect.foreach(println)")
    table.select(Tags).rightOuterJoin(other).collect.foreach(println)
  }

}
