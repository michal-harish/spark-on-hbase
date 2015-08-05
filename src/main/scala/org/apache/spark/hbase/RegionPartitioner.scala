package org.apache.spark.hbase

/**
 * Created by mharis on 09/06/15.
 */

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.keyspace.Key
import org.apache.spark.Partitioner

class RegionPartitioner(val numRegions: Int) extends Partitioner {

  private val minValue: Array[Byte] = ByteUtils.parseUUID("00000000-0000-0000-0000-000000000000")
  private val maxValue: Array[Byte] = ByteUtils.parseUUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
  val splitKeys: Array[Array[Byte]] = Bytes.split(minValue, maxValue, numRegions - 1).slice(0, numRegions)

  //HBase creates the first and last regions to be [null..startKey] and [endKey..null]
  //which would be always empty in case of min and max UUID
  val startKey: Array[Byte] = splitKeys(1)
  val endKey: Array[Byte] = splitKeys(splitKeys.length - 1)

  override def toString: String = s"RegionPartitioner(${numRegions})"

  override def numPartitions = numRegions

  override def equals(other: Any): Boolean = other match {
    case h: RegionPartitioner => h.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode: Int = numPartitions

  override def getPartition(key: Any): Int = {
    key match {
      case keyValue: KeyValue => findRegion(keyValue.getRowArray, keyValue.getRowOffset, keyValue.getRowLength)
      case key: Key => findRegion(key.bytes)
      case a: Array[Byte] if a.length > 0 => findRegion(a)
      case s: String if (s.length > 0) => findRegion(ByteUtils.reverse(s.getBytes))
      case _ => throw new IllegalArgumentException("RegionPartitioner only supports KeyValue, Key, String and Array[Byte] keys")
    }
  }

  /*
   * optimised recursive quick-sort-like lookup into the splitKeys array i.e. compare the middle index
   */
  private def findRegion(key: Array[Byte]): Int = findRegion(key, 0, key.length)

  private def findRegion(key: Array[Byte], keyOffset: Int, keyLength: Int): Int = findRegion(0, splitKeys.size - 1, key, keyOffset, keyLength)

  private def findRegion(min: Int, max: Int, key: Array[Byte], keyOffset: Int, keyLength: Int): Int = {
    if (max == min) {
      min
    } else if (max == min + 1) {
      val maxKey = splitKeys(max)
      if (ByteUtils.compare(maxKey, 0, maxKey.length, key, keyOffset, keyLength) <= 0) {
        max
      } else {
        min
      }
    } else {
      val mid = (min + max) / 2
      val midKey = splitKeys(mid)
      ByteUtils.compare(midKey, 0, midKey.length, key, keyOffset, keyLength) match {
        case c: Int if (c <= 0) => findRegion(mid, max, key, keyOffset, keyLength)
        case c: Int if (c > 0) => findRegion(min, mid - 1, key, keyOffset, keyLength)
      }
    }
  }
}
