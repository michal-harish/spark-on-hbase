package org.apache.spark.hbase.examples.graph

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
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

  implicit val DemoHKeySapceReg = Map(
    new DemoKeySpace("d").keyValue,
    new HKeySpaceUUID("u").keyValue
  )

  val graph = new HGraph(context, "demo-graph")

  implicit val partitioner = graph.partitioner



  def help = {
    println("Spark-on-HBase Graph Demo shell help:")
    println(" help - print this usage information")
    println(" create - create the underlying HBase table for the graph object")
    println(" graph - reference to the main graph instance (HGraph extends HBaseTable) ")
  }

  def create = {
    Utils.createIfNotExists(sc, graph.tableNameAsString, numRegions = 256, graph.schema:_*)
  }

  /**
   * From an adjacency lists represented as coma-separated ids to a redundant NETWORK
   */
  final def fromTextList(he: HE, textFile: RDD[String], keySpace: String): graph.NETWORK = {
    fromList(he, textFile.map(_.split(",").map(HKey(keySpace, _)).toSeq))
  }

  /**
   * From undriected adjacency lists creates a redundant directed network graph
   * in: RDD[(id1,id2)]
   */
  final def fromList(he: HE, in: RDD[Seq[HKey]]): graph.NETWORK = {
    graph.deduplicate(
      in.flatMap(a => {
        val sortedEdges = a.sorted.map(v => (v, he))
        for (id <- a) yield ((id, sortedEdges.filter(_._1 != id)))
      }))
  }
  //  /**
  //   * Used for post-splitting. Because we manage region splits manually when the table grows large
  //   * we need to 1) create new table with more regions 2) copy the old table to the new one
  //   */
  //  def copy(src: HBaseTable, dest: HBaseTable) {
  //    dest.createIfNotExists
  //    val broadCastConf = new SerializableWritable(hbaConf)
  //    val srcTableNameAsString = src.tableNameAsString
  //    val destTableNameAsString = dest.tableNameAsString
  //    val updateCount = context.accumulator(0L, "HGraph Net Update Counter")
  //    println(s"HBATable COPYING ${srcTableNameAsString} TO ${destTableNameAsString}")
  //    val srcTransformed = src.rdd().partitionBy(new RegionPartitioner(dest.numberOfRegions))
  //    srcTransformed.foreachPartition(part => {
  //      val connection = ConnectionFactory.createConnection(broadCastConf.value)
  //      val destTable = connection.getBufferedMutator(TableName.valueOf(destTableNameAsString))
  //      try {
  //        var partCount = 0L
  //        part.foreach {
  //          case (vid, result) => {
  //            val scanner = result.cellScanner()
  //            val put = new Put(vid.bytes)
  //            put.setDurability(Durability.SKIP_WAL)
  //            while (scanner.advance) {
  //              val cell = scanner.current
  //              put.add(cell)
  //            }
  //            partCount += 1
  //            destTable.mutate(put)
  //          }
  //        }
  //        updateCount += partCount
  //      } finally {
  //        destTable.close
  //      }
  //    })
  //  }
  //
  //  final def transform: Unit = {
  //    HGraphII.dropIfExists
  //    HGraphII.createIfNotExists
  //    val migrate = (old: Array[Byte]) => {
  //      val oldIdSpace = (((old(old.length - 2) & 0xff) << 8) + (old(old.length - 1) & 0xff)).toShort
  //      HKeySpace(oldIdSpace) match {
  //        case is: HKeySpaceLongHash => HKey(oldIdSpace, ByteUtils.asLongValue(old, 4).toString)
  //        case is: HKeySpaceLongPositive => HKey(oldIdSpace, (ByteUtils.asLongValue(old, 0) >>> 1).toString)
  //        case is: HKeySpaceLong => HKey(oldIdSpace, ByteUtils.asLongValue(old, 0).toString)
  //        case is: HKeySpaceString => HKey(oldIdSpace, new String(old.slice(4, old.length - 2)))
  //        case is: HKeySpaceUUID => HKey(oldIdSpace, ByteUtils.UUIDToString(old, 0))
  //        case is: HKeySpaceUUIDNumeric => HKey(oldIdSpace, ByteUtils.UUIDToNumericString(old, 0))
  //        case is: HKeySpaceUUIDNumericNoLeadZeros => HKey(oldIdSpace, ByteUtils.UUIDToNumericString(old, 0).dropWhile(_ == '0'))
  //      }
  //    }: HKey
  //    val migrateRdd: NETWORK = HGraph.rdd().map({ case (hbaseRowKey, hBaseCells) => {
  //      val newVid = migrate(hbaseRowKey.get)
  //      val edgeSeqBuilder = Seq.newBuilder[(HKey, HE)]
  //      val scanner = hBaseCells.cellScanner
  //      while (scanner.advance) {
  //        val kv = scanner.current
  //        val vid = migrate(CellUtil.cloneQualifier(kv))
  //        val ts = kv.getTimestamp
  //        edgeSeqBuilder += ((vid, HE.applyVersion(CellUtil.cloneValue(kv), ts)))
  //      }
  //      (newVid, edgeSeqBuilder.result)
  //    }
  //    })
  //    HGraphII.loadNet(migrateRdd, closeContextOnExit = true, completeAsync = false)
  //  }

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
