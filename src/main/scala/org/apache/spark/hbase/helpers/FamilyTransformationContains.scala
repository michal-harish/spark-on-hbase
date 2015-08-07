package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.filter._
import org.apache.spark.hbase.HBaseQuery

/**
 * Created by mharis on 05/08/15.
 */

class FamilyTransformationContains[K, V](t: FamilyTransformation[K,V], key: K)
  extends TransformationFilter[Map[K,V]](t) {

  override def configureQuery(query: HBaseQuery) {
    val f = new SingleColumnValueFilter(
      t.family,
      t.applyCellInverse(key, null.asInstanceOf[V])._1,
      CompareFilter.CompareOp.NOT_EQUAL,
      new NullComparator())
    f.setFilterIfMissing(true)
    query.addFilter(f)
    query.addFamily(t.family)
  }
}
