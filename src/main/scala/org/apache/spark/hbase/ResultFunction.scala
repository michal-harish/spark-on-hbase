package org.apache.spark.hbase

import org.apache.hadoop.hbase.client.{Put, Result}

/**
 * Created by mharis on 05/08/15.
 */
abstract class ResultFunction[V](val cols: String*) extends Function[Result, V] with Serializable {
  def apply(v1: Result): V

  def applyInverse(value: V, mutation: Put) = {}
}
