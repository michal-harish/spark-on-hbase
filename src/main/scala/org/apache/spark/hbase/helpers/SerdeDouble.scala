package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.Serde

/**
 * Created by mharis on 07/08/15.
 */
trait SerdeDouble extends Serde[Double] {

  override def toBytes = (value: Double) => Bytes.toBytes(value)

  override def fromBytes = (bytes: Array[Byte], o: Int, l: Int) => Bytes.toDouble(bytes, o)
}
