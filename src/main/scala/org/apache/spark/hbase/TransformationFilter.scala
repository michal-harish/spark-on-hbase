package org.apache.spark.hbase.helpers

import org.apache.spark.hbase.{HBaseQuery, Transformation}

/**
 * Created by mharis on 05/08/15.
 *
 */
abstract class TransformationFilter[V](val t: Transformation[V]) extends Serializable {

  def configureQuery(query: HBaseQuery)

}
