package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by mharis on 06/08/15.
 */
case class TDouble(column: String) extends ColumnTransformation[Double](column) {

  final override def apply(result: Result): Double = {
    val cell = result.getColumnLatestCell(family, qualifier)
    Bytes.toDouble(cell.getValueArray, cell.getValueOffset)
  }

  final override def applyInverse(value: Double, mutation: Put) = {
    mutation.addColumn(family, qualifier, Bytes.toBytes(value))
  }

}

