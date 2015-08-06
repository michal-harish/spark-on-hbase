package org.apache.spark.hbase.helpers

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.Transformation

/**
 * Created by mharis on 06/08/15.
 */
abstract class ColumnTransformation[V](private val column: String) extends Transformation[V](column) {

  val Array(family:Array[Byte], qualifier:Array[Byte]) = column.split(":").map(Bytes.toBytes(_))

}
