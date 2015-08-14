package org.apache.spark.hbase.keyspace

import org.apache.spark.hbase.keyspace.KeySpaceRegistry.KSREG
import org.apache.spark.hbase.misc.ByteUtils

class Key(val keySpace: Short, val bytes: Array[Byte], val hash: Int)(implicit reg: KSREG)
extends Serializable with Ordered[Key] {
  override def compareTo(that: Key): Int = {
    ByteUtils.compare(this.bytes, 0, this.bytes.length, that.bytes, 0, that.bytes.length)
  }

  override def compare(that: Key): Int = compareTo(that)

  override def hashCode: Int = hash

  override def equals(other: Any) = other != null && other.isInstanceOf[Key] && equals(other.asInstanceOf[Key])

  def equals(that: Key) = (this.hash == that.hash) && ByteUtils.equals(this.bytes, that.bytes)

  override def toString = KeySpace(keySpace).toString(bytes)

  def asString = KeySpace(keySpace).asString(bytes)
}

object Key extends Serializable {

  def apply(keySpace: String, id: String)(implicit reg: KSREG): Key = apply(KeySpace(keySpace), id)
  
  def apply(keySpace: Short, id: String)(implicit reg: KSREG): Key = apply(KeySpace(keySpace).asBytes(id))

  def apply(key: Array[Byte])(implicit reg: KSREG): Key = {
    val keySpace: Short = KeySpace(key, 0, key.length)
    new Key(keySpace, key, ByteUtils.asIntValue(key))
  }

  def comparator(a: Key, b: Key) = a.compareTo(b) < 0

  def higher(a: Key, b: Key): Key = if (a.compareTo(b) >= 0) a else b

  def higher[T](a: (Key,T), b: (Key,T)) : (Key, T) = if (a._1.compareTo(b._1) >= 0) a else b

  def highest(a: Key, bc: Iterable[Key]): Key = bc.foldLeft(a)((b, c) => if (b.compareTo(c) >= 0) b else c)

  def highest[T](a: (Key,T), bc: Iterable[(Key,T)]): (Key, T) = if (bc.isEmpty) a else {
    bc.foldLeft(a)((b, c) => if (b._1.compareTo(c._1) >= 0) b else c)
  }

  def highest[T](bc: Iterable[(Key,T)]): (Key, T) = if (bc.isEmpty) null else {
    bc.reduce((b, c) => if (b._1.compareTo(c._1) >= 0) b else c)
  }

}




