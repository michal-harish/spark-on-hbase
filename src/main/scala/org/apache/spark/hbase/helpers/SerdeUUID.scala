package org.apache.spark.hbase.helpers

import java.util.UUID

import org.apache.spark.hbase.{ByteUtils, KeySerDe}

/**
 * Created by mharis on 06/08/15.
 */
trait SerdeUUID extends KeySerDe[UUID] {
  final override def keyToBytes = (key: UUID) => {
    val bytes = new Array[Byte](16)
    ByteUtils.putLongValue(key.getMostSignificantBits, bytes, 0)
    ByteUtils.putLongValue(key.getLeastSignificantBits, bytes, 8)
    bytes
  }

  final override def bytesToKey = (bytes: Array[Byte]) => new UUID(
    ByteUtils.asLongValue(bytes, 0), ByteUtils.asLongValue(bytes, 8)
  )
}
