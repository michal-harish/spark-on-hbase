package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.filter.{NullComparator, CompareFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.{Serde, HBaseQuery, Transformation}

/**
 * Created by mharis on 06/08/15.
 */
case class TStringDouble(cf: String) extends FamilyTransformationCase(cf, new SerdeString() {}, new SerdeDouble() {})

case class TStringInt(cf: String) extends FamilyTransformationCase(cf, new SerdeString() {}, new SerdeInt() {})

case class TStringLong(cf: String) extends FamilyTransformationCase(cf, new SerdeString() {}, new SerdeLong() {})

case class TStringNull(cf: String) extends FamilyTransformationCase(cf, new SerdeString() {}, new SerdeNull() {})

abstract class FamilyTransformation[K, V](private val cf: String) extends Transformation[Map[K, V]](cf) {

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
    }
    }
  }

  def applyCell(cell: Cell): (K, V)

  def applyCellInverse(key: K, value: V): (Array[Byte], Array[Byte])

  def contains(key: K) = new TransformationFilter[Map[K, V]](this) {
    override def configureQuery(query: HBaseQuery) {
      query.addFamily(family)
      val q = applyCellInverse(key, null.asInstanceOf[V])._1
      val f = new SingleColumnValueFilter(family, q, CompareFilter.CompareOp.NOT_EQUAL, new NullComparator())
      f.setFilterIfMissing(true)
      query.addFilter(f)
    }
  }

  def contains(key: K, value: V) = new TransformationFilter[Map[K, V]](this) {
    override def configureQuery(query: HBaseQuery) {
      query.addFamily(family)
      val (q, v) = applyCellInverse(key, value)
      val f = new SingleColumnValueFilter(family, q, CompareFilter.CompareOp.EQUAL, v)
      f.setFilterIfMissing(true)
      query.addFilter(f)
    }
  }

}

class FamilyTransformationCase[K, V](cf: String, serde1: Serde[K], serde2: Serde[V]) extends FamilyTransformation[K, V](cf) {
  override def applyCell(cell: Cell): (K, V) = {
    val key = serde1.fromBytes(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
    val value = serde2.fromBytes(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
    (key, value)
  }

  override def applyCellInverse(key: K, value: V): (Array[Byte], Array[Byte]) = {
    (serde1.toBytes(key), serde2.toBytes(value))
  }
}
