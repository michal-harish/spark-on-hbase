package org.apache.spark.hbase.keyspace

import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkContext
import org.apache.spark.hbase.{HBaseRDD, HBaseTable}
import org.apache.spark.hbase.keyspace.KeySpaceRegistry.KSREG


/**
 * Created by mharis on 21/07/15.
 */

class HBaseTableKS(sc: SparkContext, tableNameAsString: String)(implicit reg: KSREG)
  extends HBaseTable[Key](sc, tableNameAsString) {

  override def keyToBytes: Key => Array[Byte] = (key: Key) => key.bytes

  override def bytesToKey: Array[Byte] => Key = (bytes: Array[Byte]) => Key(bytes)

  def rdd(rowKeySpace: Short): HBaseRDDKS[Result] = {
    rdd((result: Result) => result, rowKeySpace)
  }

  protected def rdd[V](valueMapper: (Result) => V, rowKeySpace: Short): HBaseRDDKS[V] = {
    new HBaseRDDKS[V](sc, tableNameAsString, rowKeySpace) {
      override def bytesToKey = HBaseTableKS.this.bytesToKey

      override def keyToBytes = HBaseTableKS.this.keyToBytes

      override def resultToValue = valueMapper
    }
  }

}
