package org.apache.spark.hbase.examples.simple

import java.util.UUID

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.spark.SparkContext
import org.apache.spark.hbase._
import org.apache.spark.hbase.helpers._

/**
 * Created by mharis on 27/07/15.
 *
 * Example
 * 'F' Column Family 'Features' - here the columns will be treated as [String -> Long] key value pairs
 * 'T' Column Family 'Tags' - only using qualifiers to have a Set[String]
 * 'S' Column Family 'Scores' - treated as [String -> Double] key value pairs
 */
object HBaseTableSimple {

  val schema = Seq(
    Utils.column("T", inMemory = false, ttlSeconds = 86400 * 90, BloomType.ROW,
      maxVersions = 1, Algorithm.SNAPPY, blocksize = 64 * 1024),
    Utils.column("F", inMemory = false, ttlSeconds = 86400 * 90, BloomType.ROWCOL,
      maxVersions = 1, Algorithm.SNAPPY, blocksize = 64 * 1024),
    Utils.column("S", inMemory = false, ttlSeconds = 86400 * 90, BloomType.ROWCOL,
      maxVersions = 1, Algorithm.SNAPPY, blocksize = 64 * 1024)
  )

}

class HBaseTableSimple(sc: SparkContext, tableNameAsString: String)
  extends HBaseTable[UUID](sc, tableNameAsString) with SerdeUUID {


  //predefined column family transformation can be declared for shorthand
  val Features = FamilyTransformation.StringLong("F")
  //using predefined transformations can be done inline - see DemoSimpleApp - e.g. TDouble("F:propensity")

  val Scores = FamilyTransformation.StringDouble("S")

  //custom transformation over multiple column families
  val CellCount = new Transformation[Short]("T", "F", "S") {
    override def apply(result: Result): Short = {
      var numCells: Int = 0
      val scanner = result.cellScanner
      while (scanner.advance) {
        numCells = numCells + 1
      }
      numCells.toShort
    }: Short
  }

  //custom tansformation of columnfamily transformation nested, i.e. we don't use the value form T column family so Set is nicer
  val Tags = new Transformation[Set[String]]("T") {
    val cfTransformation = FamilyTransformation.StringNull("T")

    def contains(tag: String) = cfTransformation.contains(tag)

    override def apply(result: Result): Set[String] = cfTransformation.apply(result).keySet

    override def applyInverse(value: Set[String], mutation: Put) {
      cfTransformation.applyInverse(value.map(x => (x -> null)).toMap, mutation)
    }
  }

}


