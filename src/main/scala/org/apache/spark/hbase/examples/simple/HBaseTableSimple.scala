package org.apache.spark.hbase.examples.simple

import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.spark.SparkContext
import org.apache.spark.hbase._

/**
 * Created by mharis on 27/07/15.
 *
 *
 */
class HBaseTableSimple(sc: SparkContext, tableNameAsString: String, cf: HColumnDescriptor*)
  extends HBaseTable[String](sc, tableNameAsString) {

  override protected def keyToBytes = (key: String) => key.getBytes

  override protected def bytesToKey = (bytes: Array[Byte]) => new String(bytes)

}

