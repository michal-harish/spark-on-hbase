package org.apache.spark.hbase.helpers

import java.util.UUID

import org.apache.spark.hbase.Serde
import org.apache.spark.hbase.misc.ByteUtils

/**
 * Created by mharis on 06/08/15.
 */
trait SerdeUUID extends Serde[UUID] {
  final override def toBytes = (key: UUID) => {
    val bytes = new Array[Byte](16)
    ByteUtils.putLongValue(key.getMostSignificantBits, bytes, 0)
    ByteUtils.putLongValue(key.getLeastSignificantBits, bytes, 8)
    bytes
  }

  final override def fromBytes = (bytes: Array[Byte], o: Int, l: Int) => new UUID(
    ByteUtils.asLongValue(bytes, o + 0), ByteUtils.asLongValue(bytes, o + 8)
  )
}
