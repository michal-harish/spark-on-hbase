package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by mharis on 06/08/15.
 */
case class TStringNull(cf: String) extends ColumnFamilyTransformation[String, Null](cf) {

  override def applyCell(cell: Cell): (String, Null) = {
    val key = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
    (key, null)
  }

  override def applyCellInverse(key: String, value: Null): (Array[Byte], Array[Byte]) = {
    (Bytes.toBytes(key), Array[Byte]())
  }

}
