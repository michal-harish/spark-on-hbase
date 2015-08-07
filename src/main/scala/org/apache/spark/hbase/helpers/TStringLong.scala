package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by mharis on 06/08/15.
 */
case class TStringLong(cf: String) extends FamilyTransformation[String, Long](cf) {

  override def applyCell(cell: Cell): (String, Long) = {
    val key = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
    val value = Bytes.toLong(cell.getValueArray, cell.getValueOffset)
    (key, value)
  }

  override def applyCellInverse(key: String, value: Long): (Array[Byte], Array[Byte]) = {
    (Bytes.toBytes(key), Bytes.toBytes(value))
  }

}
