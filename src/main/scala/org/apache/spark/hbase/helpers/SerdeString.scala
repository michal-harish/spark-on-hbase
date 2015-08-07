package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.KeySerDe

/**
 * Created by mharis on 06/08/15.
 */
trait SerdeString extends KeySerDe[String] {

  final override def keyToBytes = (key: String) => Bytes.toBytes(key)

  final override def bytesToKey = (bytes: Array[Byte]) => Bytes.toString(bytes)

}