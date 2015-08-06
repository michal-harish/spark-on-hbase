package org.apache.spark.hbase.helpers

import org.apache.spark.hbase.KeyTransformation

/**
 * Created by mharis on 06/08/15.
 */
trait KeyDefault extends KeyTransformation[Array[Byte]] {
  final override def keyToBytes = (key: Array[Byte]) => key

  final override def bytesToKey = (bytes: Array[Byte]) => bytes
}
