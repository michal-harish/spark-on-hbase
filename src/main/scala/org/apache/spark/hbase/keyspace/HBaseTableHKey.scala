package org.apache.spark.hbase.keyspace

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.keyspace.HKeySpaceRegistry.HKSREG
import org.apache.spark.hbase.{ByteUtils, HBaseTable}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext


/**
 * Created by mharis on 21/07/15.
 *
 */

class HBaseTableHKey(hbaConf: Configuration, tableNameAsString: String, numberOfRegions: Int, cfDescriptors: HColumnDescriptor*)
                    (implicit reg: HKSREG)
  extends HBaseTable[HKey](hbaConf, tableNameAsString, numberOfRegions, cfDescriptors:_*) {

  override protected def keyToBytes = (key: HKey) => key.bytes
  protected def bytesToKey = (bytes: Array[Byte]) => HKey(bytes)


  def rdd()(implicit context: SparkContext): RDD[(HKey, Result)] = new HBaseRddHKey(context, tableName)

  def rdd(cf: Array[Byte], maxStamp: Long)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRddHKey(context, tableName, HConstants.OLDEST_TIMESTAMP, maxStamp, Bytes.toString(cf))
  }

  def rdd(columns: String*)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRddHKey(context, tableName, columns: _*)
  }

  def rdd(keyIdSpace: Short, columns: String*)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRddHKey(context, tableName, keyIdSpace, columns: _*)
  }

  def rdd(keyIdSpace: Short, cf: Array[Byte], maxStamp: Long)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRddHKey(context, tableName, keyIdSpace, HConstants.OLDEST_TIMESTAMP, maxStamp, Bytes.toString(cf))
  }

  final def get(vid: HKey*): Array[Result] = {
    val connection = ConnectionFactory.createConnection(hbaConf)
    try {
      val table = connection.getTable(TableName.valueOf(tableNameAsString))
      val result = multiget(table, vid.map(_.bytes), Seq())
      table.close
      result
    } finally {
      connection.close
    }
  }

}
