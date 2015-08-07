package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by mharis on 06/08/15.
 */
case class TStringInt(cf: String) extends FamilyTransformation[String, Int](cf) {

  override def applyCell(cell: Cell): (String, Int) = {
    val key = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
    val value = Bytes.toInt(cell.getValueArray, cell.getValueOffset)
    (key, value)
  }

  override def applyCellInverse(key: String, value: Int): (Array[Byte], Array[Byte]) = {
    (Bytes.toBytes(key), Bytes.toBytes(value))
  }

}

