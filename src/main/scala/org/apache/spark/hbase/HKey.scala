package org.apache.spark.hbase

class HKey(val keySpace: Short, val bytes: Array[Byte], val hash: Int)
extends java.io.Serializable with Ordered[HKey] {
  override def compareTo(that: HKey): Int = {
    ByteUtils.compare(this.bytes, 0, this.bytes.length, that.bytes, 0, that.bytes.length)
  }

  override def compare(that: HKey): Int = compareTo(that)

  override def hashCode: Int = hash

  override def equals(other: Any) = other != null && other.isInstanceOf[HKey] && equals(other.asInstanceOf[HKey])

  def equals(that: HKey) = (this.hash == that.hash) && ByteUtils.equals(this.bytes, that.bytes)

  override def toString = HKeySpace(keySpace).toString(bytes)

  def asString = HKeySpace(keySpace).asString(bytes)
}

object HKey {

  def apply(keySpace: String, id: String): HKey = apply(HKeySpace(keySpace), id)
  
  def apply(keySpace: Short, id: String): HKey = apply(HKeySpace(keySpace).asBytes(id))

  def apply(key: Array[Byte]): HKey = {
    val keySpace: Short = HKeySpace(key, 0, key.length)
    new HKey(keySpace, key, ByteUtils.asIntValue(key))
  }

  def comparator(a: HKey, b: HKey) = a.compareTo(b) < 0

  def higher(a: HKey, b: HKey): HKey = if (a.compareTo(b) >= 0) a else b

  def higher[T](a: (HKey,T), b: (HKey,T)) : (HKey, T) = if (a._1.compareTo(b._1) >= 0) a else b

  def highest(a: HKey, bc: Iterable[HKey]): HKey = bc.foldLeft(a)((b, c) => if (b.compareTo(c) >= 0) b else c)

  def highest[T](a: (HKey,T), bc: Iterable[(HKey,T)]): (HKey, T) = if (bc.isEmpty) a else {
    bc.foldLeft(a)((b, c) => if (b._1.compareTo(c._1) >= 0) b else c)
  }

  def highest[T](bc: Iterable[(HKey,T)]): (HKey, T) = if (bc.isEmpty) null else {
    bc.reduce((b, c) => if (b._1.compareTo(c._1) >= 0) b else c)
  }

}




