package org.apache.spark.hbase.examples.simple

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.hbase.Utils
import org.apache.spark.hbase.helpers.TLong
import org.apache.spark.rdd.RDD

import scala.util.Random

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
    println(" generate  - put 1000 random rows into the underlying hbase table `demo-simple` using Transformation")
    println(" collect - collect all data from the demo table")
    println(" collectNumCells - count number of cells per row in a specialised HBaseRDD[String, Short]")
    println(" collectFeatures - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Long]]")
    println(" collectHeight - collect column `height` from family `F` as a specialised HBaseRDD[String, Long]")
    println(" collectTags - collect column family F from the demo table and map it to specialised HBaseRDD[String, Map[String,Double]]")
    println(" collectTagsAndFeatures - collect both column families T and F from the demo table and map it to specialised HBaseRDD[String, (List[String], Map[String,Double])]")
    println(" join - example join")
    println(" filter - example transformation filter")
  }

  def generate = {

    val r = new Random
    val possibleTags = Set("lego", "music", "cars", "cinema", "sport")

    val data: RDD[(UUID, Map[String, Long], Set[String])] = sc.parallelize(for (i <- (1 to 1000)) yield {
      (UUID.randomUUID(),
        Map("width" -> ((r.nextGaussian * 50) + 1000).toLong, "height" -> ((r.nextGaussian * 50) + 1000).toLong),
        possibleTags.filter(x => r.nextBoolean))
    })

    println("Updating `demo-simple` column family `F` - Features")
    table.update(table.Features, data.map { case (key, features, tags) => (key, features) })

    println("Updating `demo-simple` column family `T` - Tags")
    table.update(table.Tags, data.map { case (key, features, tags) => (key, tags) })

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
    println("> val area = table.select(TLong(\"F:width\"), TLong(\"F:height\")).sample(0.01).mapValues { case (w, h) => w * h }")
    println("> table.select(table.Tags).join(other).collect.foreach(println)")
    val area = table.select(TLong("F:width"), TLong("F:height")).sample(0.01).mapValues { case (w, h) => w * h }
    table.select(table.Tags).join(area).collect.foreach(println)
  }

  def filter = {
    println("> table.select(table.Features).filter(table.Tags contains \"lego\").collect.foreach(println)")
    table.select(table.Features).filter(table.Tags contains "lego").collect.foreach(println)
  }
}
