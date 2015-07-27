package org.apache.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.util.{Pair, Bytes}
import org.apache.spark.{SerializableWritable, TaskContext, Partition, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client._

/**
 * Created by mharis on 26/07/15.
 */
abstract class HBaseRdd[K, V](sc: SparkContext
               , @transient val tableName: TableName
               , val minStamp: Long
               , val maxStamp: Long
               , val columns: String*) extends RDD[(K, V)](sc, Nil) {

  @transient private val hbaseConf: Configuration = Utils.initConfig(sc, HBaseConfiguration.create)
  protected val tableNameAsString = tableName.toString
  protected val configuration = new SerializableWritable(hbaseConf)
  protected val regionSplits: Array[(Array[Byte], Array[Byte])] = Utils.getRegionSplits(hbaseConf, tableName)

  protected def mapKey: Array[Byte] => K

  protected def mapValue: Result => V

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
            current = Some((mapKey(result.getRow), mapValue(result)))
            true
          }
        } else {
          throw new IllegalStateException
        }
      }
    }
  }
}
