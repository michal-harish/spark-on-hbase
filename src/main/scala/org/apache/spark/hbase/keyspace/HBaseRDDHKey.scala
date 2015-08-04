package org.apache.spark.hbase.keyspace

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.HConstants._
import org.apache.hadoop.hbase.filter.FuzzyRowFilter
import org.apache.spark.SparkContext
import org.apache.spark.hbase.HBaseRDD
import org.apache.spark.hbase.keyspace.HKeySpaceRegistry.HKSREG
import scala.collection.JavaConverters._


/**
 * Created by mharis on 10/07/15.
 *
 * HBaseRDDHKey is an HBaseRDD[(HKey, hbase.client.Result)] - HKey is defined only in the scope of this special package
 * and its purpose is to allow mixing different key types in the same table and preserve even distribution across regions.
 *
 * In any HKey instance, the first 4 bytes are salt for the key generated by the particular HKeySpace to
 * which the HKey belongs and the following 2 bytes are the signature of the HKeySpace. With this representation it
 *
 * is possible to ensure that:
 * 1) any type of key can be "made" to be distributed evenly
 * 2) different key types can be mixed in a single hbase table (but don't have to be - depends on application)
 * 3) fuzzy row filter can be applied on the 2-byte key space signature to fast forward on hbase server-side
 *
 * columns is a sequence of string identifiers which can either reference a column family, e.g. 'N' or a specific
 * column, e.g. 'F:propensity'
 */
class HBaseRDDHKey(@transient private val sc: SparkContext
               , tableNameAsString: String
               , consistency: Consistency
               , keySpace: Short
               , minStamp: Long
               , maxStamp: Long
               , columns: String*
                )(implicit reg: HKSREG)
  extends HBaseRDD[HKey, Result](sc, tableNameAsString, consistency,
    minStamp, maxStamp, columns:_*) {

  def this(sc: SparkContext, tableNameAsString: String, columns: String*)(implicit reg: HKSREG)
  = this(sc, tableNameAsString, Consistency.STRONG, -1.toShort, OLDEST_TIMESTAMP, LATEST_TIMESTAMP, columns: _*)

  def this(sc: SparkContext, tableNameAsString: String, consistency: Consistency, columns: String*)(implicit reg: HKSREG)
  = this(sc, tableNameAsString, consistency, -1.toShort, OLDEST_TIMESTAMP, LATEST_TIMESTAMP, columns: _*)

  def this(sc: SparkContext, tableNameAsString: String, minStamp: Long, maxStamp: Long, columns: String*)(implicit reg: HKSREG)
  = this(sc, tableNameAsString, Consistency.STRONG, -1.toShort, minStamp, maxStamp, columns: _*)

  def this(sc: SparkContext, tableNameAsString: String, keySpace: Short, columns: String*)(implicit reg: HKSREG)
  = this(sc, tableNameAsString, Consistency.STRONG, keySpace, OLDEST_TIMESTAMP, LATEST_TIMESTAMP, columns: _*)

  override def bytesToKey = (rowKey: Array[Byte]) => HKey(rowKey)

  override def keyToBytes = (key: HKey) => key.bytes

  override def resultToValue = (row: Result) => row

  override def configureRegionScan(scan :Scan) = {
    val fuzzyRowfilter = new org.apache.hadoop.hbase.util.Pair(HKeySpace(keySpace).allocate(0), Array[Byte](1, 1, 1, 1, 0, 0))
    scan.setFilter(new FuzzyRowFilter(List(fuzzyRowfilter).asJava))
  }
}
