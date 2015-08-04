package org.apache.spark.hbase.examples.simple

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
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
    Utils.column("T", inMemory = false, ttlSeconds = 86400 * 90, BloomType.ROW,
      maxVersions = 1, Algorithm.SNAPPY, blocksize = 64 * 1024),
    Utils.column("F", inMemory = false, ttlSeconds = 86400 * 90, BloomType.ROWCOL,
      maxVersions = 1, Algorithm.SNAPPY, blocksize = 64 * 1024)
  )

}

class HBaseTableSimple(sc: SparkContext, tableNameAsString: String) extends HBaseTable[String](sc, tableNameAsString) {

  override def keyToBytes = (key: String) => Bytes.toBytes(key)

  override def bytesToKey = (bytes: Array[Byte]) => Bytes.toString(bytes)

  val Tags = new HBaseFunction[List[String]]("T") {
    val T = Bytes.toBytes("T")
    override def apply(result: Result): List[String] = {
      {
        val tagList = List.newBuilder[String]
        val scanner = result.cellScanner
        while (scanner.advance) {
          val kv = scanner.current
          if (CellUtil.matchingFamily(kv, T)) {
            val tag = Bytes.toString(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength)
            tagList += tag
          }
        }
        tagList.result
      }
    }

    override def applyInverse(value: List[String], mutation: Put) {
      value.foreach { case (tag) => {
        mutation.addColumn(T, Bytes.toBytes(tag), Array[Byte]())
      }}
    }
  }

  val Features = new HBaseFunction[Map[String, Double]]("F") {
    val F = Bytes.toBytes("F")

    override def apply(result: Result): Map[String, Double] = {
      val featureMapBuilder = Map.newBuilder[String, Double]
      val scanner = result.cellScanner
      while (scanner.advance) {
        val kv = scanner.current
        if (CellUtil.matchingFamily(kv, F)) {
          val feature = Bytes.toString(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength)
          val value = Bytes.toDouble(kv.getValueArray, kv.getValueOffset)
          featureMapBuilder += ((feature, value))
        }
      }
      featureMapBuilder.result
    }
    override def applyInverse(value: Map[String, Double], mutation: Put) {
      value.foreach { case (feature, value) => {
        mutation.addColumn(F, Bytes.toBytes(feature), Bytes.toBytes(value))
      }}
    }
  }

  val CellCount = new HBaseFunction[Short]("T", "F") {
    override def apply(result: Result): Short = {
      var numCells: Int = 0
      val scanner = result.cellScanner
      while (scanner.advance) {
        numCells = numCells + 1
      }
      numCells.toShort
    }: Short
  }

  val Propensity = new HBaseFunction[Double]("F:propensity") {
    val F = Bytes.toBytes("F")
    val propensity = Bytes.toBytes("propensity")

    override def apply(result: Result): Double = {
      val cell = result.getColumnLatestCell(F, propensity)
      Bytes.toDouble(cell.getValueArray, cell.getValueOffset)
    }
    override def applyInverse(value: Double, mutation: Put) {
      mutation.addColumn(F, propensity, Bytes.toBytes(value))
    }

  }

}


