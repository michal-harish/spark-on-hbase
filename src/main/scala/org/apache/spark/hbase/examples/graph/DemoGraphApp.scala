package org.apache.spark.hbase.examples.graph

import org.apache.hadoop.hbase.client.{Put, ConnectionFactory, Durability}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.hbase.misc.HBaseAdminUtils
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.hbase._
import org.apache.spark.hbase.keyspace._
import org.apache.spark.rdd.RDD

/**
 * class DemoApp is a starting point for spark-shell and spark-submit. When launched as a spark-shell
 * the initialisation demo-init.scala script passed instantiates DemoApp and imports all it's members to the top-level scope.
 *
 * When launched as a spark-submit, the companion object DemoApp is started
 */
class DemoGraphApp(sc: SparkContext) {

  implicit val context = sc

  implicit val DemoKeySapceReg = Map(
    new DemoKeySpace("d").keyValue,
    new KeySpaceUUID("u").keyValue
  )

  val graph = new HGraphTable(context, "demo-graph")

  implicit val partitioner = graph.partitioner

  def help = {
    println("Spark-on-HBase Graph Demo shell help:")
    println(" help - print this usage information")
    println(" create - create the underlying HBase table for the graph object")
    println(" graph - reference to the main graph instance (HGraph extends HBaseTable) ")
  }

  def create = {
    HBaseAdminUtils.updateSchema(sc, graph.tableNameAsString, numRegions = 256, graph.schema: _*)
  }

  def generate = {
    val subnet: graph.NETWORK = graph.fromPairs(sc.parallelize(Array(
      (Key("u", "83bd04a6-9219-4814-be12-bfb14476b030"), (Key("u", "ab79a709-d6b5-4d30-bf20-dc826580cbd1"), EP("DT1", 1.0))),
      (Key("u", "83bd04a6-9219-4814-be12-bfb14476b030"), (Key("d", "e7fa8bc9a44d788789b854f5c62529"), EP("P1", 0.5)))
    )))
    graph.updateNet(subnet)
  }
  /**
   * From an adjacency lists represented as coma-separated ids to a redundant NETWORK
   */
  final def fromTextList(he: EP, textFile: RDD[String], keySpace: String): graph.NETWORK = {
    fromList(he, textFile.map(_.split(",").map(Key(keySpace, _)).toSeq))
  }

  /**
   * From undriected adjacency lists creates a redundant directed network graph
   * in: RDD[(id1,id2)]
   */
  final def fromList(he: EP, in: RDD[Seq[Key]]): graph.NETWORK = {
    graph.deduplicate(
      in.flatMap(a => {
        val sortedEdges = a.sorted.map(v => (v, he))
        for (id <- a) yield ((id, sortedEdges.filter(_._1 != id)))
      }))
  }

}

/**
 * object DemoApp is an execution start point for spark-submit
 */
object DemoGraphApp extends App {
  /** Execution sequence **/
  try {
    if (args.length == 0) {
      throw new IllegalArgumentException
    }
    val context = new SparkContext() // hoping to get all configuration passed from scripts/spark-submit
    val demo = new DemoGraphApp(context)
    try {
      val a = args.iterator
      while (a.hasNext) {
        a.next match {
          case arg: String if (!arg.startsWith("-")) => {
            try {
              val methodArgs = arg.split(" ")
              if (methodArgs.length == 1) {
                val m = demo.getClass.getMethod(methodArgs(0))
                time(m.invoke(demo))
              } else {
                val m = demo.getClass.getMethod(methodArgs(0), classOf[String])
                time(m.invoke(demo, methodArgs(1)))
              }
            } catch {
              case e: NoSuchMethodException => println(s"method `${args(0)}` not defined in the DXPJobRunner")
            }
          }
        }
      }
    } finally {
      context.stop
    }
  } catch {
    case e: IllegalArgumentException => {
      println("Usage:")
      println("./spark-submit [-p] [-l|-xl|-xxl]  \"<command [argument]>\" ")
    }
  }

  def time[A](a: => A): A = {
    val l = System.currentTimeMillis
    val r = a
    println((System.currentTimeMillis - l).toDouble / 1000)
    r
  }

}
