package org.apache.spark.hbase.demo

import org.apache.spark.SparkContext
import org.apache.spark.hbase._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object DEMO extends HBAGraph with SparkUtils {

  var partitionerInstance: RegionPartitioner = null

  override def partitioner: RegionPartitioner = partitionerInstance


  final def init(sc: SparkContext) {
    initHBase(sc)
    val isLocal = sc.getConf.get("spark.master", "local").startsWith("local")
    val numPartitions = sc.getConf.getInt("spark.yarn.executor.memoryOverhead", 384) match {
      case c: Int if c >= 1024 => 8192
      case c: Int if c >= 512 => 4096
      case c: Int if c == 385 => 512
      case _ => 2048
    }
    println("numPartitions = " + numPartitions)
    this.context = sc
    this.ssc = new StreamingContext(context, Seconds(2))
    this.partitionerInstance = new RegionPartitioner(numPartitions)
  }

  var ssc: StreamingContext = null

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
  //
  //  def startStreaming() = ssc.start
  //
  //  def stopStreaming() = ssc.stop(false)
  //  def msgStream[T <: VDNAMessagePartner]()(implicit t: scala.reflect.Manifest[T]): DStream[T] = {
  //    //val stream = KafkaUtils.createStream(ssc, "bl-message-s01", "spark-streaming-test", Map("userpermissions" -> 1)).map(_._2)
  //    val stream = ssc.socketTextStream("localhost", 9999)
  //    stream.transform(rdd => rdd.mapPartitions(part => {
  //      val uni = new VDNAUniversalDeserializer
  //      part.map(message => uni.decodeString(message))
  //        .filter(x => t.erasure.isInstance(x))
  //        .map(x => t.erasure.cast(x).asInstanceOf[T])
  //    }))
  //  }
  //
  //  def syncStream(idSpace: String): DStream[(Vid, Vid)] = {
  //    msgStream[VDNAUserImport].filter(_.getIdSpace().equals(idSpace))
  //      .map(msg => (Vid(msg.getUserUid().toString), Vid(msg.getIdSpace(), msg.getPartnerUserId())))
  //  }
  //
  //  def syncStream(): DStream[(Vid, Vid)] = {
  //    msgStream[VDNAUserImport].filter(x => Vid.idSpaces.exists(y => y._2 == x.getIdSpace))
  //      .map(msg => (Vid(msg.getUserUid().toString), Vid(msg.getIdSpace(), msg.getPartnerUserId())))
  //  }


}
