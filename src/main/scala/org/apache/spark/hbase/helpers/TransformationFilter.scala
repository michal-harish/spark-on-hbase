package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.filter._
import org.apache.spark.hbase.Transformation

/**
 * Created by mharis on 05/08/15.
 *
 * demo-simple demonstration:
 * table.select(table.Features).filter(table.Features contains "width").collect.foreach(println)
 */
abstract class TransformationFilter[V](val t: Transformation[_]) extends Serializable {

  def createFilter: Filter
  
}

class TransformationFilterCONTAINS[K, V](t: ColumnFamilyTransformation[K,V], key: K)
  extends TransformationFilter[Map[K,V]](t) {

  override def createFilter: Filter = {
    val f = new SingleColumnValueFilter(
      t.family,
      t.applyCellInverse(key, null.asInstanceOf[V])._1,
      CompareFilter.CompareOp.NOT_EQUAL,
      new NullComparator())
    f.setFilterIfMissing(true)
    f
  }
}
