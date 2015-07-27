package org.apache.spark.hbase.keyspace

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.hbase.HBaseTable
import org.apache.spark.hbase.keyspace.HKeySpaceRegistry.HKSREG
import org.apache.spark.rdd.RDD


/**
 * Created by mharis on 21/07/15.
 *
 */

class HBaseTableHKey(sc: SparkContext, tableNameAsString: String, numberOfRegions: Int, cfDescriptors: HColumnDescriptor*)
                    (implicit reg: HKSREG)
  extends HBaseTable[HKey](sc, tableNameAsString, numberOfRegions, cfDescriptors:_*) {

  override protected def keyToBytes = (key: HKey) => key.bytes
  protected def bytesToKey = (bytes: Array[Byte]) => HKey(bytes)

  def rdd(keyIdSpace: Short, columns: String*): RDD[(HKey, Result)] = {
    new HBaseRDDHKey(sc, tableName, keyIdSpace, columns: _*)
  }

  def rdd(keyIdSpace: Short, cf: Array[Byte], maxStamp: Long): RDD[(HKey, Result)] = {
    new HBaseRDDHKey(sc, tableName, keyIdSpace, HConstants.OLDEST_TIMESTAMP, maxStamp, Bytes.toString(cf))
  }


}
