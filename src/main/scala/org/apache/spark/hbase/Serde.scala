package org.apache.spark.hbase

/**
 * Created by mharis on 06/08/15.
 */
trait Serde[K] extends Serializable {
  def toBytes: K => Array[Byte]

  def fromBytes: (Array[Byte], Int, Int) => K

  def fromBytes(bytes: Array[Byte]): K = fromBytes(bytes, 0, bytes.length)

}
