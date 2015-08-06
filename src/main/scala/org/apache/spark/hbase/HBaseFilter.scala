package org.apache.spark.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter, FilterList}

/**
 * Created by mharis on 05/08/15.
 */
trait HBaseFilter extends Serializable {

  def configureQuery(query: HBaseQuery): Unit;

}


class HBaseQuery(query: Query) {
  def addFilter(filter: Filter): Unit = {
    query.getFilter match {
      case null => query.setFilter(filter)
      case chain: FilterList => chain.addFilter(filter)
      case one: Filter => query.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, one, filter))
    }
  }

  def setConsistency(consistency: Consistency): Unit = query.setConsistency(consistency)

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