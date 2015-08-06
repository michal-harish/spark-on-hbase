package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by mharis on 06/08/15.
 */
case class TLong(column: String) extends ColumnTransformation[Long](column) {

  final override def apply(result: Result): Long = {
    val cell = result.getColumnLatestCell(family, qualifier)
    Bytes.toLong(cell.getValueArray, cell.getValueOffset)
  }

  final override def applyInverse(value: Long, mutation: Put) = {
    mutation.addColumn(family, qualifier, Bytes.toBytes(value))
  }

}
