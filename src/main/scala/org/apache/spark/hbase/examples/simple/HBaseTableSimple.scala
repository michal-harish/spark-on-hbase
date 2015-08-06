package org.apache.spark.hbase.examples.simple

import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.hbase._
import org.apache.spark.hbase.helpers.{TStringDouble, TDouble, ColumnFamilyTransformation, KeyString}

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

class HBaseTableSimple(sc: SparkContext, tableNameAsString: String)
  extends HBaseTable[String](sc, tableNameAsString) with KeyString {

  //using predefined transformations can be done inline - see DemoSimpleApp
  //TDouble("F:propensity")

  //predefined column family transformation can be declared for shorthand
  val Features = TStringDouble("F")

  //custom transformation over multiple column families
  val CellCount = new Transformation[Short]("T", "F") {
    override def apply(result: Result): Short = {
      var numCells: Int = 0
      val scanner = result.cellScanner
      while (scanner.advance) {
        numCells = numCells + 1
      }
      numCells.toShort
    }: Short
  }

  val Tags = new Transformation[List[String]]("T") {
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
      }
      }
    }
  }

}


