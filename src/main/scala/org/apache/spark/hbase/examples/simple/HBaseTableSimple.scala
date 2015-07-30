package org.apache.spark.hbase.examples.simple

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants._
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

class HBaseTableSimple(sc: SparkContext, tableNameAsString: String) extends HBaseTable[String](sc, tableNameAsString) {


  override protected def keyToBytes = (key: String) => key.getBytes

  override protected def bytesToKey = (bytes: Array[Byte]) => new String(bytes)

  def rddNumCells: HBaseRDD[String, Short] = rdd(OLDEST_TIMESTAMP, LATEST_TIMESTAMP).mapValues(result => {
    var numCells: Int = 0
    val scanner = result.cellScanner
    while (scanner.advance) {
      numCells = numCells + 1
    }
    numCells.toShort
  })

  def rddTags: HBaseRDD[String, List[String]] = rdd(OLDEST_TIMESTAMP, LATEST_TIMESTAMP, "T").mapValues((row: Result) => {
    val tagList = List.newBuilder[String]
    val scanner = row.cellScanner
    while (scanner.advance) {
      val kv = scanner.current
      val tag = Bytes.toString(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength)
      tagList += tag
    }
    tagList.result
  })


  def rddFeatures = rdd(OLDEST_TIMESTAMP, LATEST_TIMESTAMP, "F").mapValues((row: Result) => {
    val featureMapBuilder = Map.newBuilder[String, Double]
    val scanner = row.cellScanner
    while (scanner.advance) {
      val kv = scanner.current
      val feature = Bytes.toString(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength)
      val value = Bytes.toDouble(kv.getValueArray, kv.getValueOffset)
      featureMapBuilder += ((feature, value))
    }
    featureMapBuilder.result
  })


  def rddPropensity = {
    val cfFeatures = Bytes.toBytes("F")
    val qPropensity = Bytes.toBytes("propensity")
    rdd(OLDEST_TIMESTAMP, LATEST_TIMESTAMP, "F:propensity").mapValues((row: Result) => {
      val cell = row.getColumnLatestCell(cfFeatures, qPropensity)
      Bytes.toDouble(cell.getValueArray, cell.getValueOffset)
    })
  }

}

