package org.apache.spark.hbase

/**
 * Created by mharis on 06/08/15.
 */
trait KeyTransformation[K] extends Serializable {
  def keyToBytes: K => Array[Byte]

  def bytesToKey: Array[Byte] => K
}
