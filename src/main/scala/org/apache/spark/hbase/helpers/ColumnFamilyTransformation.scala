package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.Transformation

/**
 * Created by mharis on 06/08/15.
 */
abstract class ColumnFamilyTransformation[K,V](private val cf: String) extends Transformation[Map[K,V]](cf) {
  val family: Array[Byte] = Bytes.toBytes(cf)
  final override def apply(result: Result): Map[K, V] = {
    val builder = Map.newBuilder[K, V]
    val scanner = result.cellScanner
    while (scanner.advance) {
      val cell = scanner.current
      if (CellUtil.matchingFamily(cell, family)) {
        builder += applyCell(cell)
      }
    }
    builder.result
  }
  final override def applyInverse(value: Map[K, V], mutation: Put) {
    value.foreach { case (feature, value) => {
      val m = applyCellInverse(feature, value)
      mutation.addColumn(family, m._1, m._2)
    }}
  }

  def applyCell(cell: Cell): (K,V)

  def applyCellInverse(key: K, value: V): (Array[Byte], Array[Byte])


}
