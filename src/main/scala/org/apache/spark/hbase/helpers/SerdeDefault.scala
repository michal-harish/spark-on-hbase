package org.apache.spark.hbase.helpers

import org.apache.spark.hbase.Serde

/**
 * Created by mharis on 06/08/15.
 */
trait SerdeDefault extends Serde[Array[Byte]] {
  final override def toBytes = (key: Array[Byte]) => key

  final override def fromBytes = (bytes: Array[Byte], o: Int, l:Int) => bytes
}
