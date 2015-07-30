package org.apache.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * Created by mharis on 26/07/15.
 */

abstract class HBaseRDD[K, V](@transient private val sc: SparkContext
                              , val tableNameAsString: String
                              , val minStamp: Long
                              , val maxStamp: Long
                              , val columns: String*) extends RDD[(K, V)](sc, Nil) {

  @transient private val tableName = TableName.valueOf(tableNameAsString)
  @transient val hbaseConf: Configuration = Utils.initConfig(sc, HBaseConfiguration.create)
  protected val configuration = new SerializableWritable(hbaseConf)
  protected val regionSplits: Array[(Array[Byte], Array[Byte])] = Utils.getRegionSplits(hbaseConf, tableName)

  val cf: Seq[Array[Byte]] = columns.map(_ match {
    case cf: String if (!cf.contains(':')) => Bytes.toBytes(cf)
    case column: String => column.split(":") match {
      case Array(cf, qualifier) => Bytes.toBytes(cf)
    }
  })

  def bytesToKey: Array[Byte] => K

  def keyToBytes: K => Array[Byte]

  def resultToValue: Result => V

  def mapResultRDD[V](resultMapper: (Result) => V) = {
    new HBaseRDD[K,V](sc, tableNameAsString, minStamp, maxStamp, columns: _*) {
      override def bytesToKey = HBaseRDD.this.bytesToKey

      override def keyToBytes: (K) => Array[Byte] = HBaseRDD.this.keyToBytes

      override def resultToValue = resultMapper
    }
  }

  protected def getRegionScan(region: Int): Scan = {
    val scan = new Scan()
    scan.setMaxVersions(1)
    scan.setConsistency(Consistency.STRONG)
    if (columns.size > 0) {
      columns.foreach(_ match {
        case cf: String if (!cf.contains(':')) => scan.addFamily(Bytes.toBytes(cf))
        case column: String => column.split(":") match {
          case Array(cf, qualifier) => scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier))
        }
      })
    }
    val (startKey, stopKey) = regionSplits(region)
    if (startKey.size > 0) scan.setStartRow(startKey)
    if (stopKey.size > 0) scan.setStopRow(stopKey)
    if (minStamp != HConstants.OLDEST_TIMESTAMP || maxStamp != HConstants.LATEST_TIMESTAMP) {
      scan.setTimeRange(minStamp, maxStamp)
    }
    scan
  }

  final override protected def getPartitions: Array[Partition] = {
    (for (i <- 0 to regionSplits.size - 1) yield {
      new Partition {
        override val index: Int = i
      }
    }).toArray
  }

  @DeveloperApi
  final override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {

    val connection = ConnectionFactory.createConnection(configuration.value)
    val table = connection.getTable(TableName.valueOf(tableNameAsString))
    val scan = getRegionScan(split.index)
    val scanner: ResultScanner = table.getScanner(scan)
    var current: Option[(K, V)] = None
    //val bytesToKey = this.bytesToKey
    //val resultToValue = this.resultToValue

    new Iterator[(K, V)] {
      override def hasNext: Boolean = current match {
        case None => forward
        case _ => true
      }

      override def next(): (K, V) = {
        if (!current.isDefined && !forward) {
          throw new NoSuchElementException
        }
        val n = current.get
        current = None
        n
      }

      private def forward: Boolean = {
        if (current.isEmpty) {
          val result = scanner.next
          if (result == null || result.isEmpty) {
            table.close
            connection.close
            false
          } else {
            current = Some((bytesToKey(result.getRow), resultToValue(result)))
            true
          }
        } else {
          throw new IllegalStateException
        }
      }
    }
  }
}

object HBaseRDD {

  implicit def hBaseRddToPairRDDFunctions[K, V](rdd: HBaseRDD[K, V])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): HBaseRDDFunctions[K, V] = {
    new HBaseRDDFunctions[K, V](rdd)
  }

  def create(sc: SparkContext, tableNameAsString: String, minStamp: Long, maxStamp: Long, columns: String*)
  = new HBaseRDD[Array[Byte], Result](sc, tableNameAsString, minStamp, maxStamp, columns:_*) {
    override def bytesToKey = (bytes: Array[Byte]) => bytes

    override def resultToValue = (result: Result) => result

    override def keyToBytes = (key: Array[Byte]) => key
  }
}
