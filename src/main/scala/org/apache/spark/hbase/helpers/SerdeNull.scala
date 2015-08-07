package org.apache.spark.hbase.helpers

import org.apache.spark.hbase.Serde

/**
 * Created by mharis on 07/08/15.
 */
trait SerdeNull extends Serde[Null] {

  override def toBytes = (K) => Array[Byte]()

  override def fromBytes = (bytes: Array[Byte], o: Int, l: Int) => null
}