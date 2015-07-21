package org.apache.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Pair
import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}

import scala.collection.JavaConverters._

/**
 * Created by mharis on 10/07/15.
 */
class HBaseRdd( sc: SparkContext
                , @transient val hbaseConf: Configuration
                , @transient val tableName: TableName
                , val idSpace: Short
                , val minStamp: Long
                , val maxStamp: Long
                , val cf: Array[Byte]*
                ) extends RDD[(HKey, Result)](sc, Nil) with HBaseDescriptors {

  private val tableNameAsString = tableName.toString
  private val configuration = new SerializableWritable(hbaseConf)

  def this (sc: SparkContext, hbaConf: Configuration, tableName: TableName, cf: Array[Byte]*)
  = this (sc, hbaConf, tableName, -1.toShort, HConstants.OLDEST_TIMESTAMP, HConstants.LATEST_TIMESTAMP, cf:_*)

  def this (sc: SparkContext, hbaConf: Configuration, tableName: TableName, minStamp: Long, maxStamp: Long, cf: Array[Byte]*)
  = this (sc, hbaConf, tableName, -1.toShort, minStamp, maxStamp, cf:_*)

  def this (sc: SparkContext, hbaConf: Configuration, tableName: TableName, idSpace: Short, cf: Array[Byte]*)
  = this (sc, hbaConf, tableName, idSpace, HConstants.OLDEST_TIMESTAMP, HConstants.LATEST_TIMESTAMP, cf:_*)

  val regionSplits: Array[(Array[Byte], Array[Byte])] = getRegionSplits(hbaseConf, tableName)

  override protected def getPartitions: Array[Partition] = {
    (for (i <- 0 to regionSplits.size - 1) yield {
      new Partition {
        override val index: Int = i
      }
    }).toArray
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(HKey, Result)] = {

    val connection = ConnectionFactory.createConnection(configuration.value)
    val table = connection.getTable(TableName.valueOf(tableNameAsString))
    val scan = new Scan()
    cf.foreach(scan.addFamily(_))
    scan.setMaxVersions(1)
    scan.setConsistency(Consistency.STRONG)
    val (startKey, stopKey) = regionSplits(split.index)
    if (startKey.size>0) scan.setStartRow(startKey)
    if (stopKey.size>0) scan.setStopRow(stopKey)
    if (minStamp != HConstants.OLDEST_TIMESTAMP || maxStamp != HConstants.LATEST_TIMESTAMP) {
      scan.setTimeRange(minStamp, maxStamp)
    }
    if (idSpace != -1) {
      scan.setFilter(new FuzzyRowFilter(
        List(new Pair(HKeySpace(idSpace).allocate(0), Array[Byte](1,1,1,1,0,0))).asJava
      ))
    }
    val scanner: ResultScanner = table.getScanner(scan)
    var current: Option[(HKey, Result)] = None

    new Iterator[(HKey, Result)] {
      override def hasNext: Boolean = current match {
        case None => forward
        case _ => true
      }

      override def next(): (HKey, Result) = {
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
            current = Some((HKey(result.getRow), result))
            true
          }
        } else {
          throw new IllegalStateException
        }
      }
    }
  }

}
