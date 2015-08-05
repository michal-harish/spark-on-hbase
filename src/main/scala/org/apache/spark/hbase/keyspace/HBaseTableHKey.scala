package org.apache.spark.hbase.keyspace

import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkContext
import org.apache.spark.hbase.HBaseTable
import org.apache.spark.hbase.keyspace.HKeySpaceRegistry.HKSREG


/**
 * Created by mharis on 21/07/15.
 */

class HBaseTableHKey(sc: SparkContext, tableNameAsString: String)(implicit reg: HKSREG)
  extends HBaseTable[HKey](sc, tableNameAsString) {

  override def keyToBytes: HKey => Array[Byte] = (key: HKey) => key.bytes

  override def bytesToKey: Array[Byte] => HKey = (bytes: Array[Byte]) => HKey(bytes)

  def rdd(rowKeySpace: Short, columns: String*): HBaseRDDHKey[Result] = {
    rdd((result: Result) => result, rowKeySpace, columns:_*)
  }

  protected def rdd[V](valueMapper: (Result) => V, rowKeySpace: Short, columns: String*): HBaseRDDHKey[V] = {
    new HBaseRDDHKey[V](sc, tableNameAsString, rowKeySpace, columns: _*) {
      override def bytesToKey = HBaseTableHKey.this.bytesToKey

      override def keyToBytes = HBaseTableHKey.this.keyToBytes

      override def resultToValue = valueMapper
    }
  }

}
