package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.Serde

/**
 * Created by mharis on 06/08/15.
 */
trait SerdeString extends Serde[String] {

  final override def toBytes = (key: String) => Bytes.toBytes(key)

  final override def fromBytes = (bytes: Array[Byte], o:Int, l:Int) => Bytes.toString(bytes, o, l)

}