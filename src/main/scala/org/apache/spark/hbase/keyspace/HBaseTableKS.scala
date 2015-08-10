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

  override def toBytes: Key => Array[Byte] = (key: Key) => key.bytes

  override def fromBytes: (Array[Byte], Int, Int) => Key = (bytes: Array[Byte], o:Int, l:Int) => Key(bytes)

  def rdd(rowKeySpace: Short): HBaseRDDKS[Result] = {
    rdd(rowKeySpace, (result: Result) => result)
  }

  protected def rdd[V](rowKeySpace: Short, valueMapper: (Result) => V): HBaseRDDKS[V] = {
    new HBaseRDDKS[V](sc, tableNameAsString, rowKeySpace) {
      override def fromBytes = HBaseTableKS.this.fromBytes

      override def toBytes = HBaseTableKS.this.toBytes

      override def resultToValue = valueMapper
    }
  }

}
