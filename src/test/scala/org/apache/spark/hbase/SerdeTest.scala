package org.apache.spark.hbase

import java.util.UUID

import org.apache.spark.hbase.helpers.{SerdeDefault, SerdeUUID}
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by mharis on 07/08/15.
 */
class SerdeTest extends FlatSpec with Matchers {

  behavior of "SerdeDefault"
  it should "be deterministic both ways" in {
    val serde = new SerdeDefault(){}
    val key = ByteUtils.parseUUID(UUID.randomUUID.toString)
    val bytes = serde.toBytes(key)
    bytes.length should be(key.length)
    ByteUtils.equals(serde.fromBytes(bytes), key) should be(true)
  }

  behavior of "SerdeUUID"
  it should "be deterministic both ways" in {
    val serde = new SerdeUUID(){}
    val key = UUID.randomUUID()
    val bytes = serde.toBytes(key)
    bytes.length should be(16)
    serde.fromBytes(bytes) should be(key)
  }


}
