package org.apache.spark.hbase.examples.simple

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HConstants}
import org.apache.spark.SparkContext
import org.apache.spark.hbase._

/**
 * Created by mharis on 27/07/15.
 *
 * Example
 * 'F' Column Family 'Features' - here the columns will be treated as [String -> Double] key value pairs
 * 'T' Column Family 'Tags' - only using qualifiers to have a Set[String]
 */
object HBaseTableSimple {
  val schema = Seq(
    Utils.column("T", inMemory = false, ttlSeconds = 86400 * 90, BloomType.ROW, maxVersions = 1, Algorithm.SNAPPY, blocksize = 64 * 1024),
    Utils.column("F", inMemory = false, ttlSeconds = 86400 * 90, BloomType.ROWCOL, maxVersions = 1, Algorithm.SNAPPY, blocksize = 64 * 1024)
  )
}

class HBaseTableSimple(sc: SparkContext, tableNameAsString: String, cf: HColumnDescriptor*)
  extends HBaseTable[String](sc, tableNameAsString) {

  override protected def keyToBytes = (key: String) => key.getBytes

  override protected def bytesToKey = (bytes: Array[Byte]) => new String(bytes)

  def rddNumCells: HBaseRDD[String, Short] = {
    val cfr = (row: Result) => {
      var numCells: Short = 0
      val scanner = row.cellScanner
      while (scanner.advance) numCells += 1
      numCells
    }
    rdd[Short](cfr, HConstants.OLDEST_TIMESTAMP, HConstants.LATEST_TIMESTAMP)
  }


  def rddTags: HBaseRDD[String, List[String]] = {
    val cfTags = Bytes.toBytes("T")
    val cfr = (row: Result) => {
      val featureMapBuilder = List.newBuilder[String]
      val scanner = row.cellScanner
      while (scanner.advance) {
        val kv = scanner.current
        if (Bytes.equals(kv.getFamilyArray, kv.getFamilyOffset, kv.getFamilyLength, cfTags, 0, cfTags.length)) {
          val feature = Bytes.toString(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength)
          featureMapBuilder += ((feature))
        }
      }
      featureMapBuilder.result
    }
    rdd[List[String]](cfr, HConstants.OLDEST_TIMESTAMP, HConstants.LATEST_TIMESTAMP, "T")
  }

  def rddFeatures = {
    val cfFeatures = Bytes.toBytes("F")
    val cfr = (row: Result) => {
      val featureMapBuilder = Map.newBuilder[String, Double]
      val scanner = row.cellScanner
      while (scanner.advance) {
        val kv = scanner.current
        if (Bytes.equals(kv.getFamilyArray, kv.getFamilyOffset, kv.getFamilyLength, cfFeatures, 0, cfFeatures.length)) {
          val feature = Bytes.toString(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength)
          val value = Bytes.toDouble(kv.getValueArray, kv.getValueOffset)
          featureMapBuilder += ((feature, value))
        }
      }
      featureMapBuilder.result
    }
    rdd[Map[String, Double]](cfr, HConstants.OLDEST_TIMESTAMP, HConstants.LATEST_TIMESTAMP, "F")
  }

  def rddPropensity = {
    val cfFeatures = Bytes.toBytes("F")
    val qPropensity = Bytes.toBytes("propensity")
    val cfr = (row: Result) => {
      val cell = row.getColumnLatestCell(cfFeatures, qPropensity)
      Bytes.toDouble(cell.getValueArray, cell.getValueOffset)
    }
    rdd[Double](cfr, HConstants.OLDEST_TIMESTAMP, HConstants.LATEST_TIMESTAMP, "F:propensity")
  }


}

