package org.apache.spark.hbase.examples.graph

import org.apache.spark.hbase.ByteUtils
import org.apache.spark.hbase.keyspace.{HKeySpace, KeySerdeUUIDNumeric}

/**
 * Created by mharis on 23/07/15.
 * This is an example how to create new key space that can be used with this spark-hbase implementation.
 * It is a representation of UUID which I've seen in real world where the dashes and leading zeros are stripped away.
 */

class DemoKeySpace(symbol: String)  extends HKeySpace(symbol) with KeySerdeUUIDNumeric {
  override def asString(bytes: Array[Byte]): String = uuidToNumericString(bytes, 6).dropWhile(_ == '0')
  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(16)
    stringToUUIDNumeric("00000000000000000000000000000000" + id takeRight 32, 0, bytes, 6)
    ByteUtils.copy(bytes, 6, bytes, 0, 4)
    bytes
  }
}
