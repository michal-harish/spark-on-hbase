package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by mharis on 06/08/15.
 */
case class TString(column: String) extends ColumnTransformation[String](column) {

  final override def apply(result: Result): String = {
    val cell = result.getColumnLatestCell(family, qualifier)
    Bytes.toString(cell.getValueArray, cell.getValueOffset)
  }

  final override def applyInverse(value: String, mutation: Put) = {
    mutation.addColumn(family, qualifier, Bytes.toBytes(value))
  }

}
