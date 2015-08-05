package org.apache.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by mharis on 26/07/15.
 */

abstract class HBaseRDD[K, V](@transient val sc: SparkContext
                              , val tableNameAsString: String
                              , val filters: Seq[HBaseFilter]) extends RDD[(K, V)](sc, Nil) {

  @transient private val tableName = TableName.valueOf(tableNameAsString)
  @transient val hbaseConf: Configuration = Utils.initConfig(sc, HBaseConfiguration.create)
  protected val configuration = new SerializableWritable(hbaseConf)
  protected val regionSplits: Array[(Array[Byte], Array[Byte])] = Utils.getRegionSplits(hbaseConf, tableName)
  @transient override val partitioner: Option[Partitioner] = Some(new RegionPartitioner(regionSplits.size))


  def bytesToKey: Array[Byte] => K

  def keyToBytes: K => Array[Byte]

  def resultToValue: Result => V

  final override protected def getPartitions: Array[Partition] = {
    (for (i <- 0 to regionSplits.size - 1) yield {
      new Partition {
        override val index: Int = i
      }
    }).toArray
  }


  final def configureQuery(query: HBaseQuery) = {
    query.setMaxVersions(1)
    filters.foreach(_.configureQuery(query))
  }

  @DeveloperApi
  final override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {

    val connection = ConnectionFactory.createConnection(configuration.value)
    val table = connection.getTable(TableName.valueOf(tableNameAsString))
    val scan = new Scan()
    val (startKey, stopKey) = regionSplits(split.index)
    if (startKey.size > 0) scan.setStartRow(startKey)
    if (stopKey.size > 0) scan.setStopRow(stopKey)
    configureQuery(scan)

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

  implicit def hBaseRddToPairRDDFunctions[K, V, H](rdd: HBaseRDD[K, V])
                                                  (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): HBaseRDDFunctions[K, V] = {
    new HBaseRDDFunctions[K, V](rdd)
  }

  def create(sc: SparkContext, tableNameAsString: String, columns: String*) = {
    new HBaseRDD[Array[Byte], Result](sc, tableNameAsString, Nil) {
      override def bytesToKey = (bytes: Array[Byte]) => bytes

      override def resultToValue = (result: Result) => result

      override def keyToBytes = (key: Array[Byte]) => key
    }.select(columns: _*)
  }
}
