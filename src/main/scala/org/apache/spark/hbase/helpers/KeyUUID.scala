package org.apache.spark.hbase.helpers

import java.util.UUID

import org.apache.spark.hbase.{ByteUtils, KeyTransformation}

/**
 * Created by mharis on 06/08/15.
 */
trait KeyUUID extends KeyTransformation[UUID] {
  final override def keyToBytes = (key: UUID) => {
    val bytes = new Array[Byte](16)
    ByteUtils.putLongValue(key.getMostSignificantBits, bytes, 0)
    ByteUtils.putLongValue(key.getLeastSignificantBits, bytes, 8);
  }

  final override def bytesToKey = (bytes: Array[Byte]) => UUID.nameUUIDFromBytes(bytes)
}
