package org.apache.spark.hbase.examples.graph

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HConstants}
import org.apache.spark.hbase._
import org.apache.spark.hbase.keyspace.HKeySpaceRegistry.HKSREG
import org.apache.spark.hbase.keyspace.{HBaseTableHKey, HKey, HKeySpace}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkContext}

import scala.collection.mutable.LinkedHashMap
import scala.reflect.ClassTag

/**
 * Created by mharis on 26/07/15.
 *
 * HE - HGraphEdge - is a lightweight object that holds the properties of each edge in the HGraph
 */
class HE(val bytes: Array[Byte], val vendorCode: Short, val ts: Long) extends Serializable with Props[HE] {

  override def combine(other: HE): HE = HE.applyVersion(ByteUtils.max(bytes, other.bytes), Math.max(other.ts, ts))

  def hbaseValue: (Array[Byte], Long) = (bytes, ts)

  def vendorUnknown: Boolean = vendorCode == HE.VENDOR_CODE_UNKNOWN

  def probability: Double = (bytes(1) & 0xFF).toDouble / 255.0

  override def toString: String = s"P=${probability}@${HE.vendors(vendorCode)}/${ts}"

  override def hashCode = ByteUtils.asIntValue(bytes)

  override def equals(other: Any) = other != null && other.isInstanceOf[HE] && ByteUtils.equals(bytes, other.asInstanceOf[HE].bytes)
}

object HE extends Serializable {
  val CURRENT_VERSION = 1.toByte
  val VENDOR_CODE_UNKNOWN = 0.toShort
  val vendors = Map[Short, String](
    VENDOR_CODE_UNKNOWN -> "UNKNOWN",
    //PROBABILISTIC
    128.toShort -> "P1",
    129.toShort -> "P2",
    //DETERMINISTIC
    250.toShort -> "DT1",
    //RESERVED
    251.toShort -> "test1",
    252.toShort -> "test2",
    253.toShort -> "test3",
    254.toShort -> "test4",
    Short.MaxValue -> "RESERVED")
  val vendors2: Map[String, Short] = vendors.map(x => (x._2 -> x._1))

  implicit def ordering[A <: (HKey, _)]: Ordering[A] = new Ordering[A] {
    override def compare(x: A, y: A): Int = x._1.compareTo(y._1)
  }

  def apply(vendorCode: Short, probability: Double, ts: Long): HE = {
    val b: Array[Byte] = new Array[Byte](4)
    b(0) = CURRENT_VERSION
    b(1) = (probability * 255.0).toByte
    b(2) = ((vendorCode >>> 8) & 0xFF).toByte
    b(3) = ((vendorCode >>> 0) & 0xFF).toByte
    new HE(b, vendorCode, ts)
  }

  def apply(vendor: String, probability: Double, ts: Long): HE = {
    if (probability < 0.0 || probability > 1.0) throw new IllegalArgumentException(s"Probability must be 0.0 >= p >= 1.0")
    if (!vendors2.contains(vendor)) throw new IllegalArgumentException(s"Unknown graph connection vendor `${vendor}`")
    apply(vendors2(vendor), probability, ts)
  }

  def apply(vendor: String, probability: Double): HE = apply(vendor, probability, HConstants.LATEST_TIMESTAMP)

  def applyVersion(bytes: Array[Byte], ts: Long): HE = {
    if (bytes.length != 4 || bytes(0) != CURRENT_VERSION) {
      apply(HE.VENDOR_CODE_UNKNOWN, 255.toByte, ts)
    } else {
      val vendorCodeParsed: Short = (((bytes(2).toShort & 0xff) << 8) + ((bytes(3).toShort & 0xff) << 0)).toShort
      if (vendors.contains(vendorCodeParsed)) {
        new HE(bytes, vendorCodeParsed, ts)
      } else {
        apply(HE.VENDOR_CODE_UNKNOWN, bytes(1), ts)
      }
    }
  }

}


class HGraphTable(sc: SparkContext, tableName: String)(implicit reg: HKSREG) extends HBaseTableHKey(sc, tableName: String)
with AGraph[HE] {

  @transient
  val schema = List(
      Utils.column("N", true, 86400 * 360, BloomType.ROW, 1, Algorithm.SNAPPY, 32 * 1024)
    , Utils.column("E", true, 86400 * 30, BloomType.ROW, 1, Algorithm.SNAPPY, 32 * 1024)
    , Utils.column("F", false, 86400 * 90, BloomType.ROWCOL, 1, Algorithm.SNAPPY, 64 * 1024))

  type FEATURES = Map[String, Array[Byte]]
  type STATS = LinkedHashMap[String, Accumulator[Long]]

  val cfNet = Bytes.toBytes("N")
  val cfEval = Bytes.toBytes("E")
  val cfFeatures = Bytes.toBytes("F")

  @transient
  val propsFile = new Path(s"/hgraph/${tableName}") //TODO configurable path for hgraph properties
  val props = scala.collection.mutable.LinkedHashMap[String, String]()

  val CFRFeatures = (row: Result) => {
    val featureMapBuilder = Map.newBuilder[String, Array[Byte]]
    val scanner = row.cellScanner
    val cfFeatures = Bytes.toBytes("F")
    while (scanner.advance) {
      val kv = scanner.current
      if (Bytes.equals(kv.getFamilyArray, kv.getFamilyOffset, kv.getFamilyLength, cfFeatures, 0, cfFeatures.length)) {
        val feature = Bytes.toString(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength)
        val value = CellUtil.cloneValue(kv)
        featureMapBuilder += ((feature, value))
      }
    }
    featureMapBuilder.result
  }

  val CFREdges = (row: Result) => {
    val edgeSeqBuilder = Seq.newBuilder[(HKey, HE)]
    val scanner = row.cellScanner
    val cfNet = Bytes.toBytes("N")
    while (scanner.advance) {
      val kv = scanner.current
      if (Bytes.equals(kv.getFamilyArray, kv.getFamilyOffset, kv.getFamilyLength, cfNet, 0, cfNet.length)) {
        val key = HKey(CellUtil.cloneQualifier(kv))
        val ts = kv.getTimestamp
        edgeSeqBuilder += ((key, HE.applyVersion(CellUtil.cloneValue(kv), ts)))
      }
    }
    edgeSeqBuilder.result
  }

  def start_date = props("start_date")

  def end_date = props("end_date")

  def loadProperties = {
    val fs = FileSystem.get(hbaseConf)
    try {
      val stream = fs.open(propsFile)
      props ++= scala.io.Source.fromInputStream(stream).getLines.map(_.split("=")).filter(_.size == 2).map(x => (x(0), x(1)))
      stream.close
    } catch {
      case e: java.io.FileNotFoundException => {
        props ++= Map("start_date" -> "2015-06-10", "end_date" -> "2015-06-17")
      }
    }
    props
  }

  def updateProperties(set: (String, String)*) = {
    props ++= set
    val fs = FileSystem.get(hbaseConf)
    fs.makeQualified(propsFile)
    val stream = new BufferedWriter(new OutputStreamWriter(fs.create(propsFile, true)))
    props.map(x => s"${x._1}=${x._2}").toSeq.foreach(line => stream.write(line + "\r\n"))
    stream.close
  }

  def rddFeatures: LAYER[FEATURES] = rdd(HKeySpace("d"), cfFeatures, HConstants.LATEST_TIMESTAMP).mapValues(CFRFeatures)

  def rddFeatures[T](feature: String)(implicit tag: ClassTag[T]): LAYER[T] = {
    val cfFeatures = this.cfFeatures
    val fFeature = Bytes.toBytes(feature)
    val filtered = rdd(HKeySpace("d"), cfFeatures, HConstants.LATEST_TIMESTAMP).filter(_._2.containsNonEmptyColumn(cfFeatures, fFeature))
    val t: ((Result) => T) = tag.runtimeClass match {
      case java.lang.Long.TYPE => (row: Result) => Bytes.toLong(row.getValue(cfFeatures, fFeature)).asInstanceOf[T]
      case java.lang.Double.TYPE => (row: Result) => Bytes.toDouble(row.getValue(cfFeatures, fFeature)).asInstanceOf[T]
      case _ => throw new UnsupportedOperationException(tag.runtimeClass.toString)
    }
    filtered.mapPartitions(rows => rows.map({ case (key, values) => (key, t(values)) }), preservesPartitioning = true)
  }

  def updateFeatures[T](x: LAYER[(String, T)])(implicit tag: ClassTag[T]) = {
    val t: (T => Array[Byte]) = tag.runtimeClass match {
      case java.lang.Long.TYPE => (value: T) => Bytes.toBytes(value.asInstanceOf[Long])
      case java.lang.Double.TYPE => (value: T) => Bytes.toBytes(value.asInstanceOf[Double])
      case _ => throw new UnsupportedOperationException
    }
    update(cfFeatures, x.mapValues(f => (Map(Bytes.toBytes(f._1) ->(t(f._2), HConstants.LATEST_TIMESTAMP)))))
  }

  def updateFeatures[T](feature: String, x: LAYER[T])(implicit tag: ClassTag[T]) = {
    val fFeature = Bytes.toBytes(feature)
    val t: (T => Array[Byte]) = tag.runtimeClass match {
      case java.lang.Long.TYPE => (value: T) => Bytes.toBytes(value.asInstanceOf[Long])
      case java.lang.Double.TYPE => (value: T) => Bytes.toBytes(value.asInstanceOf[Double])
      case _ => throw new UnsupportedOperationException
    }
    update(cfFeatures, x.mapValues(f => (Map(fFeature ->(t(f), HConstants.LATEST_TIMESTAMP)))))
  }

  def incFeature(feature: String, incRdd: LAYER[Long]) {
    super.increment(cfFeatures, Bytes.toBytes(feature), incRdd)
  }

  def copyNet(dest: HGraphTable, closeContextOnExit: Boolean = false): Long = {
    val update: RDD[(HKey, EDGES)] = rddNet
      .leftOuterJoin(dest.rddNet)
      .mapValues({ case (left, right) => right match {
      case None => left
      case Some(r) => left.filter(l => !r.contains(l))
    }
    })
    dest.loadNet(update, closeContextOnExit)
  }

  def histogram(beforeTimestamp: Long = HConstants.LATEST_TIMESTAMP): Array[(Long, Long)] = hist(rddNumEdges(beforeTimestamp))

  def rddNumEdges(beforeTimestamp: Long = HConstants.LATEST_TIMESTAMP): LAYER[Long] = {
    val cfNet = Bytes.toBytes("N")
    val CFRNumEdges = (row: Result) => {
      val scanner = row.cellScanner
      var numEdges = 0L
      while (scanner.advance) numEdges += 1L
      numEdges
    }
    rdd(HConstants.OLDEST_TIMESTAMP, beforeTimestamp, "N").mapValues(CFRNumEdges)
  }

  /**
   * Repair - 1) unknown vendors 2) TODO broken links
   */
  def repairNet(update: Boolean, closeContextOnExit: Boolean): NETWORK = {
    val rdd: NETWORK = rddNet.filter(_._2.exists(_._2.vendorUnknown)).mapValues { edges => {
      edges.map(edge => if (edge._2.vendorUnknown) (edge._1, HE("AAT", 1.0, edge._2.ts)) else edge)
    }
    }
    if (update) {
      loadNet(rdd, closeContextOnExit)
    }
    rdd
  }

  /**
   * rddPool Returns a POOL of all Row keys in the given key space and their maximum connected Row in the same key space
   */
  def rddPool(keySpace: String, beforeTimestamp: Long = HConstants.LATEST_TIMESTAMP): POOL = {
    val cfNet = this.cfNet
    val keySpaceCode = HKeySpace(keySpace)
    val CFRMaxKey1Space = (row: Result) => {
      val scanner = row.cellScanner
      var selectedCell: Cell = null
      while (scanner.advance) {
        val kv = scanner.current
        if (HKeySpace(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength) == keySpaceCode) {
          selectedCell = kv
        }
      }
      HKey(if (selectedCell == null) row.getRow else CellUtil.cloneQualifier(selectedCell))
    }
    rdd(keySpaceCode, cfNet, beforeTimestamp).mapValues(CFRMaxKey1Space)
  }

  /**
   * rddNet is the RDD representation of the underlying hbase state of the N (NETWORK) column family
   * it includes the singletons which will have an empty Seq() in the value
   */
  def rddNet: NETWORK = rddNet("*", HConstants.LATEST_TIMESTAMP)

  def rddNet(keySpace: String, beforeTimestamp: Long = HConstants.LATEST_TIMESTAMP): NETWORK = {
    val cfNet = this.cfNet
    val keySpaceCode = HKeySpace(keySpace)
    val allSpaces = keySpace == "*"
    val CFREdge1S = (row: Result) => {
      val edgeSeqBuilder = Seq.newBuilder[(HKey, HE)]
      val scanner = row.cellScanner
      while (scanner.advance) {
        val kv = scanner.current
        if (allSpaces || HKeySpace(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength) == keySpaceCode) {
          val key = HKey(CellUtil.cloneQualifier(kv))
          val ts = kv.getTimestamp
          edgeSeqBuilder += ((key, HE.applyVersion(CellUtil.cloneValue(kv), ts)))
        }
      }
      edgeSeqBuilder.result
    }
    (if (allSpaces) rdd(HConstants.OLDEST_TIMESTAMP, beforeTimestamp, "N") else rdd(keySpaceCode, cfNet, beforeTimestamp))
      .mapValues(CFREdge1S)
  }


//  def joinNet[R: ClassTag](rightSideRdd: LAYER[R], multi: Int): LAYER[(EDGES, R)] = {
//    TODO new HBaseRDDEdges(this).join(rightSideRdd:Layer[R]): LAYER[(EDGES, R)] = {.. }
//  }

//  def outerJoinNet[R: ClassTag](rightSideRdd: LAYER[R], multi: Int): LAYER[(Option[EDGES], R)] = {
//    outerLookupNet(rightSideRdd.mapValues(x => (None, x)), multi)
//  }
//
//  def outerLookupNet[R: ClassTag](rightSideRdd: LAYER[(Option[EDGES], R)], multi: Int): LAYER[(Option[EDGES], R)] = {
//    val j = /*TODO if (multi == -1) new HBaseLookupRangeScan[EDGES, R](cfNet) else */ new HBaseLookupMultiGet[EDGES, R](multi, cfNet)
//    j(CFREdges, rightSideRdd)
//  }

//  def filter[X: ClassTag](preSorted: Boolean, rdd: LAYER[X]): LAYER[X] = {
//    if (preSorted) {
//      new HBaseJoinRangeScan[EDGES, X](cfNet).apply(CFREdges, rdd).mapValues(_._2)
//    } else {
//      new HBaseJoinMultiGet[EDGES, X](10000, cfNet).apply(CFREdges, rdd).mapValues(_._2)
//    }
//  }

  def loadPairs(update: PAIRS, completeAsync: Boolean = true): Long = {
    super.bulkLoad(cfNet, reverse(update).mapValues{ case (key, he) => Map(key.bytes -> he.hbaseValue)}, completeAsync)
  }

  def updateNetRdd(update: NETWORK) = update.mapValues(_.map { case (key, he) => (key.bytes, he.hbaseValue) }.toMap)

  def updateNet(rdd: NETWORK): Long = super.update(cfNet, updateNetRdd(rdd))

  def loadNet(rdd: NETWORK, completeAsync: Boolean = true): Long = {
    super.bulkLoad(cfNet, updateNetRdd(rdd), completeAsync)
  }

//  def loadNet(bsp: BSP_RESULT,  completeAsync: Boolean): Unit = {
//    val (update, stats, history) = bsp
//    try {
//      loadNet(update.mapValues(_._2), completeAsync)
//    } finally {
//      stats.foreach({ case (superstep, stat) => println(s"BSP ${superstep} STATS ${stat.name} = ${stat.value}") })
//      history.foreach(_.unpersist(false))
//    }
//  }

//  def remove(ids: HKey*)(implicit context: SparkContext): Long = {
//    val j = new HBaseJoinMultiGet[EDGES, HKey](1000, cfNet)
//    delete(cfNet, j(CFREdges, context.parallelize(ids.map(x => (x, null.asInstanceOf[HKey]))))
//      .flatMap { case (key, (edges, right)) => edges.map(e => (e._1, Set(key.bytes))) :+(key, Set(null.asInstanceOf[Array[Byte]])) }
//      .reduceByKey(_ ++ _).mapValues(_.toSeq)
//    )
//  }
//
//  def remove[T: ClassTag](ids: RDD[(HKey, T)],  completeAsync: Boolean = true): Long = {
//    val j = new HBaseJoinMultiGet[EDGES, HKey](1000, cfNet)
//    removeNet(j(CFREdges, ids.mapValues(x => null.asInstanceOf[HKey])).mapValues(_._1), completeAsync)
//  }

  def removeNet(rdd: NETWORK, completeAsync: Boolean = true): Long = {
    val d = rdd.flatMap { case (key, edges) => edges.map(e => (e._1, Set(key.bytes))) :+(key, Set(null.asInstanceOf[Array[Byte]])) }
      .reduceByKey(_ ++ _).mapValues(_.toSeq)
    bulkDelete(cfNet, d, completeAsync)
  }

//  /**
//   * This is a stateless BSP algorithm that propagates connection with deteriorating probabilities
//   * - it uses recursive RDD mapping without modifying HBase
//   * - state update is left to the caller by the returned rdd of 'suggested changes'.
//   * - it the RDD immutability and resiliency however
//   */
//  //---------(Key--(PrevState,-----Pending-Inbox)))
//  type BSP = (HKey, (Option[EDGES], (EDGES, EDGES)))
//  //----------------[(Key, (State, Update)]
//  type BSP_OUT = RDD[(HKey, (EDGES, EDGES))]
//  type BSP_RESULT = (BSP_OUT, STATS, HISTORY)
//
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
