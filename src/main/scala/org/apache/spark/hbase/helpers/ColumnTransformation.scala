package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.{HBaseQuery, Serde, Transformation}

/**
 * Created by mharis on 06/08/15.
 */
case class TBool(column: String) extends ColumnTransformation[Boolean](column) with SerdeBool

case class TInt(column: String) extends ColumnTransformation[Int](column) with SerdeInt

case class TLong(column: String) extends ColumnTransformation[Long](column) with SerdeLong

case class TDouble(column: String) extends ColumnTransformation[Double](column) with SerdeDouble

case class TString(column: String) extends ColumnTransformation[String](column) with SerdeString

abstract class ColumnTransformation[V](private val column: String) extends Transformation[V](column) with Serde[V] {

  val Array(family:Array[Byte], qualifier:Array[Byte]) = column.split(":").map(Bytes.toBytes(_))

  final override def apply(result: Result): V = {
    val cell = result.getColumnLatestCell(family, qualifier)
    fromBytes(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }

  final override def applyInverse(value: V, mutation: Put) =mutation.addColumn(family, qualifier, toBytes(value))

  def < (value: V) = compare(CompareFilter.CompareOp.LESS, value)

  def <= (value: V) = compare(CompareFilter.CompareOp.LESS_OR_EQUAL, value)

  def =< (value: V) = compare(CompareFilter.CompareOp.LESS_OR_EQUAL, value)

  def == (value: V) = compare(CompareFilter.CompareOp.EQUAL, value)

  def >= (value: V) = compare(CompareFilter.CompareOp.GREATER_OR_EQUAL, value)

  def > (value: V) = compare(CompareFilter.CompareOp.GREATER, value)

  protected def compare(op: CompareFilter.CompareOp, value: V ) = new TransformationFilter[V](this) {
    override def configureQuery(query: HBaseQuery) {
      query.addColumn(family, qualifier)
      val f = new SingleColumnValueFilter(family, qualifier, op, toBytes(value))
      f.setFilterIfMissing(true)
      query.addFilter(f)
    }
  }
}

