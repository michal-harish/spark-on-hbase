package org.apache.spark.hbase

import java.util

import org.apache.hadoop.hbase.client._

/**
 * Created by mharis on 05/08/15.
 */
trait HBaseFilter extends Serializable {

  def configureQuery(query: HBaseQuery);

}

class HBaseQuery(query: Query) extends Query {
  def addColumn(family: Array[Byte], qualifier: Array[Byte]): Unit = query match {
    case scan: Scan => scan.addColumn(family, qualifier)
    case get: Get => get.addColumn(family, qualifier)
  }

  def addFamily(family: Array[Byte]): Unit = query match {
    case scan: Scan => scan.addFamily(family)
    case get: Get => get.addFamily(family)
  }

  def setTimeRange(minStamp: Long, maxStamp: Long) = query match {
    case scan: Scan => scan.setTimeRange(minStamp, maxStamp)
    case get: Get => get.setTimeRange(minStamp, maxStamp)
  }

  def setMaxVersions(i: Int) = query match {
    case scan: Scan => scan.setMaxVersions(i)
    case get: Get => get.setMaxVersions(i)
  }

  override def toMap(i: Int): util.Map[String, AnyRef] = query.toMap

  override def getFingerprint: util.Map[String, AnyRef] = query.getFingerprint

}

object HBaseQuery {
  implicit def scanToHBaseQuery(scan: Scan) = new HBaseQuery(scan)
  implicit def getToHBaseQuery(get: Get) = new HBaseQuery(get)
}