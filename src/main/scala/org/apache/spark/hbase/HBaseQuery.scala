package org.apache.spark.hbase

import org.apache.hadoop.hbase.client.{Get, Scan, Consistency, Query}
import org.apache.hadoop.hbase.filter.{FilterList, Filter}

/**
 * Created by mharis on 05/08/15.
 */
class HBaseQuery(query: Query) {
  def addFilter(filter: Filter): Unit = {
    query.getFilter match {
      case null => query.setFilter(filter)
      case chain: FilterList => chain.addFilter(filter)
      case one: Filter => query.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, one, filter))
    }
  }

  def setConsistency(consistency: Consistency): Unit = query.setConsistency(consistency)

  /* TODO it would be nice to use setFamilyMap for scans by collecting required columns in an internal array
  def setFamilyMap(familyMap: Map[Array[Byte], Set[Array[Byte]]]): Unit = {
    query match {
      case scan: Scan => scan.setFamilyMap(familyMap.mapValues(x => new util.TreeSet[Array[Byte]](x.asJava).asInstanceOf[util.NavigableSet[Array[Byte]]]).asJava)
      case get: Get => familyMap.foreach { case (family, set) => set.foreach(qualifier => get.addColumn(family, qualifier)) }
    }
  }
  */

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

}

object HBaseQuery {
  implicit def scanToHBaseQuery(scan: Scan) = new HBaseQuery(scan)

  implicit def getToHBaseQuery(get: Get) = new HBaseQuery(get)
}