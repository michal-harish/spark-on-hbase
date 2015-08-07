package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.Serde

/**
 * Created by mharis on 07/08/15.
 */
trait SerdeLong extends Serde[Long] {

  override def toBytes = (value: Long) => Bytes.toBytes(value)

  override def fromBytes = (bytes: Array[Byte], o:Int, l:Int) => Bytes.toLong(bytes, o)

}
