package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.Serde

/**
 * Created by mharis on 07/08/15.
 */
trait SerdeBool extends Serde[Boolean] {
  final override def toBytes = (value: Boolean) => Bytes.toBytes(value)

  final override def fromBytes = (bytes: Array[Byte], o: Int, l: Int) => Bytes.toBoolean(bytes)
}
