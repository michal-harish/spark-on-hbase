package org.apache.spark.hbase.keyspace

import org.apache.hadoop.hbase.HConstants._
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkContext
import org.apache.spark.hbase.HBaseTable
import org.apache.spark.hbase.keyspace.HKeySpaceRegistry.HKSREG
import org.apache.spark.rdd.RDD


/**
 * Created by mharis on 21/07/15.
 *
 */

class HBaseTableHKey(sc: SparkContext, tableNameAsString: String)(implicit reg: HKSREG)
  extends HBaseTable[HKey](sc, tableNameAsString) {

  override def keyToBytes: HKey => Array[Byte] = (key: HKey) => key.bytes

  override def bytesToKey: Array[Byte] => HKey = (bytes: Array[Byte]) => HKey(bytes)

  def rdd(rowKeySpace: Short, columns: String*): RDD[(HKey, Result)] = {
    rdd(Consistency.STRONG, rowKeySpace, columns: _*)
  }

  def rdd(rowKeySpace: Short, minStamp:Long, maxStamp: Long, columns: String*): HBaseRDDHKey = {
    rdd(Consistency.STRONG, rowKeySpace, minStamp, maxStamp, columns: _*)
  }
  def rdd(consistency: Consistency, rowKeySpace: Short, columns: String*): HBaseRDDHKey = {
    rdd(consistency, rowKeySpace, OLDEST_TIMESTAMP, LATEST_TIMESTAMP, columns: _*)
  }

  def rdd(consistency: Consistency, rowKeySpace: Short, minStamp:Long, maxStamp: Long, columns: String*): HBaseRDDHKey = {
    new HBaseRDDHKey(sc, tableNameAsString, Consistency.STRONG, rowKeySpace, OLDEST_TIMESTAMP, maxStamp, columns:_*)
  }

}
