package org.apache.spark.hbase.examples.graph

import org.apache.hadoop.hbase.HConstants._
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.spark.hbase._
import org.apache.spark.hbase.keyspace.KeySpaceRegistry.KSREG
import org.apache.spark.hbase.keyspace.{HBaseTableKS, Key, KeySpace}
import org.apache.spark.hbase.misc.HBaseAdminUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkContext}

import scala.collection.mutable.LinkedHashMap
import scala.reflect.ClassTag

class HGraphTable(sc: SparkContext, tableName: String)(implicit reg: KSREG) extends HBaseTableKS(sc, tableName: String)
with AGraph[EP] {

  @transient
  val schema = List(
    HBaseAdminUtils.column("N", true, 86400 * 360, BloomType.ROW, 1, Algorithm.SNAPPY, 32 * 1024)
    , HBaseAdminUtils.column("E", true, 86400 * 30, BloomType.ROW, 1, Algorithm.SNAPPY, 32 * 1024))

  type STATS = LinkedHashMap[String, Accumulator[Long]]

  case class Edges(keySpace: String) extends Transformation[Seq[EDGE]]("N") {
    val N = Bytes.toBytes("N")
    val keySpaceCode = KeySpace(keySpace)
    val allSpaces = keySpace == "*"

    override def apply(result: Result): Seq[(Key, EP)] = {
      val edgeSeqBuilder = Seq.newBuilder[(Key, EP)]
      val scanner = result.cellScanner
      val cfNet = Bytes.toBytes("N")
      while (scanner.advance) {
        val kv = scanner.current
        if (Bytes.equals(kv.getFamilyArray, kv.getFamilyOffset, kv.getFamilyLength, cfNet, 0, cfNet.length)) {
          if (allSpaces || KeySpace(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength) == keySpaceCode) {
            val key = Key(CellUtil.cloneQualifier(kv))
            val ts = kv.getTimestamp
            edgeSeqBuilder += ((key, EP.applyVersion(CellUtil.cloneValue(kv), ts)))
          }
        }
      }
      edgeSeqBuilder.result
    }

    override def applyInverse(edges: Seq[EDGE], mutation: Put) = {
      //TODO use keySpace if not allSpaces
      edges.foreach { case (dest: Key, props: EP) => {
        mutation.addColumn(N, toBytes(dest), props.ts, props.bytes)
      }
      }
    }
  }

  val NumEdges = new Transformation[Long]("N") {
    override def apply(result: Result): Long = {
      val scanner = result.cellScanner
      var numEdges = 0L
      while (scanner.advance) numEdges += 1L
      numEdges
    }
  }

  case class MaxConnected(keySpace: String) extends Transformation[Key]("N") {
    val keySpaceCode = KeySpace(keySpace)

    override def apply(result: Result): Key = {
      val scanner = result.cellScanner
      var selectedCell: Cell = null
      while (scanner.advance) {
        val kv = scanner.current
        if (KeySpace(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength) == keySpaceCode) {
          selectedCell = kv
        }
      }
      Key(if (selectedCell == null) result.getRow else CellUtil.cloneQualifier(selectedCell))
    }
  }


  def histogram(beforeTimestamp: Long = LATEST_TIMESTAMP): Array[(Long, Long)] = {
    hist(select(NumEdges).filter(OLDEST_TIMESTAMP, beforeTimestamp))
  }

  /**
   * rddPool Returns a POOL of all Row keys in the given key space and their maximum connected Row in the same key space
   */
  def rddPool(keySpace: String): POOL = rdd(KeySpace(keySpace), MaxConnected(keySpace)).select("N")

  //TODO this.rdd.filter(KeySpace(keySpace)).select(MaxConnected(keySpace))
  //OR this.select(MaxConnected(keySpace)).filter(KeySpace(keySpace))

  /**
   * rddNet is the RDD representation of the underlying hbase state of the N (NETWORK) column family
   * it includes the singletons which will have an empty Seq() in the value
   */
  def rddNet: NETWORK = rddNet("*")

  def rddNet(keySpace: String): NETWORK = rdd(KeySpace(keySpace), Edges(keySpace)).select("N")

  def loadPairs(update: PAIRS, completeAsync: Boolean = true): Long = {
    super.bulkLoad(Bytes.toBytes("N"), reverse(update).mapValues {
      case (key, he) => Map(key.bytes -> he.hbaseValue)
    }, completeAsync)
  }

  def updateNet(rdd: NETWORK) = super.update(Edges("*"), rdd)

  def loadNet(rdd: NETWORK, completeAsync: Boolean = true): Long = super.bulkUpdate(Edges("*"), rdd, completeAsync)

  def loadNet(bsp: BSP_RESULT, completeAsync: Boolean): Unit = {
    val (update, stats, history) = bsp
    try {
      loadNet(update.mapValues(_._2), completeAsync)
    } finally {
      stats.foreach({ case (superstep, stat) => println(s"BSP ${superstep} STATS ${stat.name} = ${stat.value}") })
      history.foreach(_.unpersist(false))
    }
  }


  def remove[T: ClassTag](ids: RDD[(Key, T)], completeAsync: Boolean = true): Long = {
    removeNet(select(Edges("*")).join(ids.mapValues(x => null.asInstanceOf[Key])).mapValues(_._1), completeAsync)
  }

  def removeNet(rdd: NETWORK, completeAsync: Boolean = true): Long = {
    val d = rdd.flatMap { case (key, edges) => edges.map(e => (e._1, Set(key.bytes))) :+(key, Set(null.asInstanceOf[Array[Byte]])) }
      .reduceByKey(_ ++ _).mapValues(_.toSeq)
    bulkDelete(Bytes.toBytes("N"), d, completeAsync)
  }

  /**
   * This is a stateless BSP algorithm that propagates connection with deteriorating probabilities
   * - it uses recursive RDD mapping without modifying HBase
   * - state update is left to the caller by the returned rdd of 'suggested changes'.
   * - it the RDD immutability and resiliency however
   */
  //---------(Key--(PrevState,-----Pending-Inbox)))
  type BSP = (Key, (Option[EDGES], (EDGES, EDGES)))
  //----------------[(Key, (State, Update)]
  type BSP_OUT = RDD[(Key, (EDGES, EDGES))]
  type BSP_RESULT = (BSP_OUT, STATS, HISTORY)

  //  def incrementalNetBSP(adj: NETWORK, numSupersteps: Int = 9, multiGetSize: Int = 1000): BSP_RESULT = {
  //    incrementalNetBSP(adj, new HBaseLookupMultiGet[EDGES, (EDGES, EDGES)](multiGetSize, cfNet), numSupersteps)
  //  }
  //
  //  def incrementalNetBSP(adj: NETWORK, lookup: HBaseLookup[EDGES, (EDGES, EDGES)], numSteps: Int)
  //                       (implicit context: SparkContext): BSP_RESULT = {
  //    println(s"STATELESS BSP [${numSteps}-STEP]")
  //    var transit: RDD[BSP] = adj.mapValues(x => (None, (Seq[EDGE](), x)))
  //    var superstep = 0
  //    val stats: STATS = LinkedHashMap[String, Accumulator[Long]]()
  //    val history = new HISTORY
  //    while (superstep < numSteps) {
  //      superstep += 1
  //      val statInbox = context.accumulator(0L, s"INBOX")
  //      stats += (s"${superstep} INBOX" -> statInbox)
  //      val statOutbox = context.accumulator(0L, s"OUTBOX")
  //      stats += (s"${superstep} OUTBOX" -> statOutbox)
  //      val input: RDD[BSP] = lookup(CFREdges, transit).mapValues {case (hbase, (pending, inbox)) => {
  //        (hbase, (pending, inbox.filter { case (eid, ehe) => {
  //          (hbase.get == null || !hbase.get.exists(p => p._1 == eid && p._2.probability >= ehe.probability))
  //        }}))
  //      }}
  //      history += input.setName(s"${superstep} INPUT").persist(StorageLevel.MEMORY_AND_DISK_SER)
  //      val superstepSnapshot = superstep
  //      //input.collect.filter(_._2._2._2.size>0).foreach(x => println(s"${superstep} INBOX ${x._1} <- ${x._2._2._2}"))
  //      val messages: RDD[BSP] = input.flatMap { case (vid, (hbase, (pending, newInbox))) => {
  //        if (newInbox.size == 0 ) {
  //          statOutbox += 1
  //          Seq(((vid, (hbase, (pending, Seq.empty)))))
  //        } else {
  //          val builder = Seq.newBuilder[BSP]
  //          //always transfer the inbox to the pending state and override pending with higher probabilities
  //          statInbox += newInbox.size
  //          builder += ((vid, (hbase, (pending.filter(p => {
  //            !newInbox.exists(e => e._1 == p._1 && e._2.probability > p._2.probability)
  //          }) ++ newInbox, Seq.empty))))
  //
  //          if (superstepSnapshot < numSteps) { //if this is the last superstep, no need to trigger further outbox
  //          val existing = if (hbase.isDefined && hbase.get != null) hbase.get ++ pending else pending
  //            if (existing.size > 0) {
  //              existing.foreach { case (s, she) => builder += ((s, (None, (Seq.empty, newInbox.map {
  //                case (eid, ehe) => (eid, HE(ehe.vendorCode, ehe.probability * she.probability, ehe.ts))
  //              }))))
  //              }
  //              newInbox.foreach { case (i, ihe) => builder += ((i, (None, (Seq.empty, existing.map {
  //                case (eid, ehe) => (eid, HE(ihe.vendorCode, ehe.probability * ihe.probability, ehe.ts))
  //              }))))
  //              }
  //            }
  //          }
  //          val seq = builder.result
  //          statOutbox += seq.size
  //          seq
  //        }
  //      }
  //      }
  //      //messages.sortByKey().collect.foreach(x => println(s"${superstep} OUTBOX ${x}"))
  //      //collapse messages - reduceByKey alternative
  //      transit = messages.reduceByKey(partitioner, (e1: (Option[EDGES], (EDGES, EDGES)), e2: (Option[EDGES], (EDGES, EDGES))) => {
  //        val hbase: Option[EDGES] = if (e1._1.isDefined) e1._1 else e2._1
  //        val pending: EDGES = if (e1._2._1.isEmpty) e2._2._1 else e1._2._1
  //        val inbox: EDGES =
  //          e1._2._2.filter(e_1 =>
  //            !e2._2._2.exists(e_2 => e_2._1 == e_1._1 && e_2._2.probability >= e_1._2.probability)
  //              && !pending.exists(p => p._1 == e_1._1 && p._2.probability >= e_1._2.probability)
  //              && (hbase.isEmpty || hbase.get == null || !hbase.get.exists(p => p._1 ==  e_1._1 && p._2.probability >= e_1._2.probability))
  //          ) ++
  //            e2._2._2.filter(e_2 => !e1._2._2.exists(e_1 => e_1._1 == e_2._1 && e_1._2.probability > e_2._2.probability)
  //              && !pending.exists(p => p._1 == e_2._1 && p._2.probability >= e_2._2.probability)
  //              && (hbase.isEmpty || hbase.get == null || !hbase.get.exists(p => p._1 ==  e_2._1 && p._2.probability >= e_2._2.probability))
  //            )
  //        (hbase, (pending, inbox))
  //      })
  //    }
  //    val output: BSP_OUT = transit.mapValues { case (state, (pending, inbox)) =>
  //      (if (state.isEmpty || state.get == null) Seq() else state.get, pending)
  //    }
  //    output.setName(s"BSP Final Output")
  //    (output, stats, history)
  //  }
}
