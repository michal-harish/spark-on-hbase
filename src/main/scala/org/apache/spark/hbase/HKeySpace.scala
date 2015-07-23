package org.apache.spark.hbase

object HKeySpace {

  def apply(id: Array[Byte], offset: Int, length: Int): Short = {
    (((id(offset + 4) & 0xff) << 8) + (id(offset + 5) & 0xff)).toShort
  }

  def apply(idSpace: String): Short = idSpace.hashCode.toShort

  def apply(idSpace: Short): HKeySpace = if (exists(idSpace)) mapping(idSpace) else throw new IllegalArgumentException

  def exists(idSpace: Short): Boolean = mapping.contains(idSpace)

  private val mapping = scala.collection.mutable.HashMap[Short, HKeySpace]()

  def register(keySpace: HKeySpace) = mapping += (keySpace.i -> keySpace)

}


abstract class HKeySpace(val symbol: String) {
  val i = symbol.hashCode.toShort

  def asBytes(id: String): Array[Byte]

  def asString(bytes: Array[Byte]): String

  final def toString(bytes: Array[Byte]): String = asString(bytes) + ":" + symbol

  def allocate(length: Int): Array[Byte] = {
    val bytes = new Array[Byte](length + 6)
    bytes(4) = (i >> 8).toByte
    bytes(5) = (i).toByte
    bytes
  }

  def keyValue: (Short, HKeySpace) = (i -> this)
}

class HKeySpaceUUID(symbol: String) extends HKeySpace(symbol) with KeySerdeUUID {
  override def asString(bytes: Array[Byte]): String = uuidToString(bytes, 6)
  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(16)
    stringToUUID(id, 0, bytes, 6)
    ByteUtils.copy(bytes, 6, bytes, 0, 4)
    bytes
  }
}

class HKeySpaceUUIDNumeric(symbol: String)  extends HKeySpace(symbol) with KeySerdeUUIDNumeric {
  override def asString(bytes: Array[Byte]): String = uuidToNumericString(bytes, 6)
  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(16)
    stringToUUIDNumeric(id, 0, bytes, 6)
    ByteUtils.copy(bytes, 6, bytes, 0, 4)
    bytes
  }
}



class HKeySpaceString(symbol: String) extends HKeySpace(symbol) with KeySerdeString {
  override def asString(bytes: Array[Byte]): String = bytesToString(bytes, 6, bytes.length)
  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(id.length)
    stringToBytes(id, 0, id.length, bytes, 6)
    ByteUtils.putIntValue(id.hashCode, bytes, 0)
    bytes
  }

}

class HKeySpaceLong(symbol: String) extends HKeySpace(symbol) with KeySerdeLong {
  override def asString(bytes: Array[Byte]): String = longBytesToString(bytes, 6)
  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(8)
    longStringToBytes(id, bytes, 6)
    ByteUtils.copy(bytes, 6, bytes, 0, 4)
    bytes
  }
}

class HKeySpaceLongPositive(symbol: String) extends HKeySpace(symbol) with KeySerdeLongPositive {
  override def asString(bytes: Array[Byte]): String = longPositiveBytesToString(bytes, 6)
  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(8)
    longPositiveStringToBytes(id, bytes, 6)
    ByteUtils.copy(bytes, 6, bytes, 0, 4)
    bytes
  }
}

trait KeySerdeUUID {
  val uuidPattern = "^(?i)[a-f0-9]{8}\\-[a-f0-9]{4}\\-[a-f0-9]{4}\\-[a-f0-9]{4}\\-[a-f0-9]{12}$".r.pattern

  def stringToUUID(id:String, srcOffset: Int, dest: Array[Byte], destOffset: Int): Array[Byte] = {
    if (uuidPattern.matcher(id).matches) ByteUtils.parseUUID(id.getBytes(), 1, srcOffset, dest, destOffset)
    else throw new IllegalArgumentException(s"UUID string format found: ${id}")
    dest
  }

  def uuidToString(src: Array[Byte], srcOffset: Int) : String = {
    ByteUtils.UUIDToString(src, srcOffset)
  }
}

trait KeySerdeUUIDNumeric {
  val uuidPatternNumeric = "^(?i)[a-f0-9]{32}$".r.pattern

  def stringToUUIDNumeric(id:String, srcOffset: Int, dest: Array[Byte], destOffset: Int) {
    if (uuidPatternNumeric.matcher(id).matches) ByteUtils.parseUUID(id.getBytes(), 0, srcOffset, dest, destOffset)
    else throw new IllegalArgumentException(s"Numeric UUID string format found: ${id}")
  }

  def uuidToNumericString(src: Array[Byte], srcOffset: Int) : String = {
    ByteUtils.UUIDToNumericString(src, srcOffset)
  }
}

trait KeySerdeString {
  def stringToBytes(id: String, srcStart: Int, srcEnd: Int, dest: Array[Byte], destOffset: Int) {
    id.getBytes(srcStart, srcEnd, dest, destOffset)
  }

  def bytesToString(src: Array[Byte], srcStart: Int, srcEnd: Int): String = {
    new String(src.slice(srcStart, srcEnd))
  }
}

trait KeySerdeLong {
  def longStringToBytes(id: String, dest: Array[Byte], destOffset: Int) {
    ByteUtils.putLongValue(ByteUtils.parseLongRadix10(id.getBytes, 0, id.length - 1), dest, destOffset)
  }

  def longBytesToString(bytes: Array[Byte], offset: Int): String = {
    ByteUtils.asLongValue(bytes, offset).toString
  }
}

trait KeySerdeLongPositive {
  def longPositiveStringToBytes(id: String, dest: Array[Byte], destOffset: Int) = {
    ByteUtils.putLongValue(ByteUtils.parseLongRadix10(id.getBytes, 0, id.length - 1) << 1, dest, destOffset)
  }

  def longPositiveBytesToString(bytes: Array[Byte], offset: Int): String = {
    (ByteUtils.asLongValue(bytes, offset) >>> 1).toString
  }
}


//TODO trait VidSerdeInt
//TODO trait VidSerdeDouble
//TODO trait VidSerdeIPV4
