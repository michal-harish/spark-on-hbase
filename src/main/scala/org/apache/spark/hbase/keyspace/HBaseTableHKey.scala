package org.apache.spark.hbase.keyspace

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
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

  @transient
  def rdd(rowKeySpace: Short, columns: String*): RDD[(HKey, Result)] = {
    new HBaseRDDHKey(sc, tableNameAsString, rowKeySpace, columns: _*)
  }

  @transient
  def rdd(rowKeySpace: Short, cf: Array[Byte], maxStamp: Long): RDD[(HKey, Result)] = {
    new HBaseRDDHKey(sc, tableNameAsString, rowKeySpace, HConstants.OLDEST_TIMESTAMP, maxStamp, Bytes.toString(cf))
  }

}
