package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by mharis on 06/08/15.
 */

case class TInt(column: String) extends ColumnTransformation[Int](column) {

  final override def apply(result: Result): Int = {
    val cell = result.getColumnLatestCell(family, qualifier)
    Bytes.toInt(cell.getValueArray, cell.getValueOffset)
  }

  final override def applyInverse(value: Int, mutation: Put) = {
    mutation.addColumn(family, qualifier, Bytes.toBytes(value))
  }

}
