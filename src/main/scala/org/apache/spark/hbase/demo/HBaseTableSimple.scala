package org.apache.spark.hbase.demo

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.hbase._
import org.apache.spark.hbase.keyspace.HKeySpaceRegistry._

/**
 * Created by mharis on 27/07/15.
 */

class HBaseTableSimple(sc: SparkContext, tableNameAsString: String)(implicit reg: HKSREG)
  extends HBaseTable[String](sc, tableNameAsString, 100,
    Utils.column(Bytes.toBytes("F"), false, 86400 * 90, BloomType.ROWCOL, 1, Algorithm.SNAPPY, 64 * 1024)) {

  override protected def keyToBytes = (key: String) => key.getBytes

  override protected def bytesToKey = (bytes: Array[Byte]) => new String(bytes)

  override def createIfNotExists: Boolean = {
    val result = super.createIfNotExists
    val insert = sc.parallelize(Array(
      "row1" -> Map(Bytes.toBytes("c1") ->(Bytes.toBytes("value1"), System.currentTimeMillis)),
      "row2" -> Map(Bytes.toBytes("c1") ->(Bytes.toBytes("value2"), System.currentTimeMillis))
    ))
    this.update(Bytes.toBytes("F"), insert) > 0 || result

  }

}