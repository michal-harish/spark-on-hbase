package org.apache.spark.hbase.keyspace

import org.apache.hadoop.hbase.HConstants._
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkContext
import org.apache.spark.hbase.{HBaseRDD, HBaseTable}
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

  def rdd(consistency: Consistency, rowKeySpace: Short,
          minStamp: Long, maxStamp: Long, columns: String*): HBaseRDDHKey[Result] = {
    rdd((result: Result) => result, consistency, rowKeySpace, minStamp, maxStamp, columns:_*)
  }

  protected def rdd[V](valueMapper: (Result) => V, consistency: Consistency, rowKeySpace: Short,
                       minStamp: Long, maxStamp: Long, columns: String*): HBaseRDDHKey[V] = {
    new HBaseRDDHKey[V](sc, tableNameAsString, consistency, rowKeySpace, minStamp, maxStamp, columns: _*) {
      override def bytesToKey = HBaseTableHKey.this.bytesToKey

      override def keyToBytes = HBaseTableHKey.this.keyToBytes

      override def resultToValue = valueMapper
    }
  }

}
