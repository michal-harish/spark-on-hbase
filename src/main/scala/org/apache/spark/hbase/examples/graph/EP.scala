package org.apache.spark.hbase.examples.graph

import org.apache.hadoop.hbase.HConstants
import org.apache.spark.hbase.ByteUtils
import org.apache.spark.hbase.keyspace.Key

/**
 * Created by mharis on 26/07/15.
 *
 * EP - Edge Properties - is a lightweight object that holds the properties of each edge in the HGraph
 */
class EP(val bytes: Array[Byte], val vendorCode: Short, val ts: Long) extends Serializable with Props[EP] {

  override def combine(other: EP): EP = EP.applyVersion(ByteUtils.max(bytes, other.bytes), Math.max(other.ts, ts))

  def hbaseValue: (Array[Byte], Long) = (bytes, ts)

  def vendorUnknown: Boolean = vendorCode == EP.VENDOR_CODE_UNKNOWN

  def probability: Double = (bytes(1) & 0xFF).toDouble / 255.0

  override def toString: String = s"P=${probability}@${EP.vendors(vendorCode)}/${ts}"

  override def hashCode = ByteUtils.asIntValue(bytes)

  override def equals(other: Any) = other != null && other.isInstanceOf[EP] && ByteUtils.equals(bytes, other.asInstanceOf[EP].bytes)
}

object EP extends Serializable {
  val CURRENT_VERSION = 1.toByte
  val VENDOR_CODE_UNKNOWN = 0.toShort
  val vendors = Map[Short, String](
    VENDOR_CODE_UNKNOWN -> "UNKNOWN",
    //PROBABILISTIC
    128.toShort -> "P1",
    129.toShort -> "P2",
    //DETERMINISTIC
    250.toShort -> "DT1",
    //RESERVED
    251.toShort -> "test1",
    252.toShort -> "test2",
    253.toShort -> "test3",
    254.toShort -> "test4",
    Short.MaxValue -> "RESERVED")
  val vendors2: Map[String, Short] = vendors.map(x => (x._2 -> x._1))

  implicit def ordering[A <: (Key, _)]: Ordering[A] = new Ordering[A] {
    override def compare(x: A, y: A): Int = x._1.compareTo(y._1)
  }

  def apply(vendorCode: Short, probability: Double, ts: Long): EP = {
    val b: Array[Byte] = new Array[Byte](4)
    b(0) = CURRENT_VERSION
    b(1) = (probability * 255.0).toByte
    b(2) = ((vendorCode >>> 8) & 0xFF).toByte
    b(3) = ((vendorCode >>> 0) & 0xFF).toByte
    new EP(b, vendorCode, ts)
  }

  def apply(vendor: String, probability: Double, ts: Long): EP = {
    if (probability < 0.0 || probability > 1.0) throw new IllegalArgumentException(s"Probability must be 0.0 >= p >= 1.0")
    if (!vendors2.contains(vendor)) throw new IllegalArgumentException(s"Unknown graph connection vendor `${vendor}`")
    apply(vendors2(vendor), probability, ts)
  }

  def apply(vendor: String, probability: Double): EP = apply(vendor, probability, HConstants.LATEST_TIMESTAMP)

  def applyVersion(bytes: Array[Byte], ts: Long): EP = {
    if (bytes.length != 4 || bytes(0) != CURRENT_VERSION) {
      apply(EP.VENDOR_CODE_UNKNOWN, 255.toByte, ts)
    } else {
      val vendorCodeParsed: Short = (((bytes(2).toShort & 0xff) << 8) + ((bytes(3).toShort & 0xff) << 0)).toShort
      if (vendors.contains(vendorCodeParsed)) {
        new EP(bytes, vendorCodeParsed, ts)
      } else {
        apply(EP.VENDOR_CODE_UNKNOWN, bytes(1), ts)
      }
    }
  }

}
