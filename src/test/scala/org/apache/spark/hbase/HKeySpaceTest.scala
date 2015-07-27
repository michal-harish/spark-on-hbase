package org.apache.spark.hbase

import java.util.UUID

import org.apache.spark.hbase.keyspace.HKeySpaceRegistry.HKSREG
import org.apache.spark.hbase.keyspace._
import org.scalatest.{Matchers, FlatSpec}

import scala.util.Random

/**
 * Created by mharis on 23/07/15.
 */
class HKeySpaceTest extends FlatSpec with Matchers {

  val random = new Random
  val numPartitions = 32
  val partitioner = new RegionPartitioner(numPartitions)

  implicit val TestHKeySpaceReg: HKSREG = Map(
    new HKeySpaceLong("l").keyValue,
    new HKeySpaceLongPositive("lp").keyValue,
    new HKeySpaceUUID("u").keyValue
  )


  behavior of "HKeySpaceLong"
  it should "have even distribution when partitioned by RegionPartitioner" in {
    val keys: Seq[HKey] = for (i <- (0 to 100000)) yield HKey("l", random.nextLong.toString)
    verifyEvenDistribution(keys)
  }

  behavior of "HKeySpaceLongPositive"
  it should "have even distribution when partitioned by RegionPartitioner" in {
    val keys: Seq[HKey] = for (i <- (0 to 100000)) yield HKey("lp", math.abs(random.nextLong).toString)
    verifyEvenDistribution(keys)
  }

  behavior of "HKeySpaceUUID"
  it should "have even distribution when partitioned by RegionPartitioner" in {

    val keys: Seq[HKey] = for (i <- (0 to 100000)) yield HKey("u", UUID.randomUUID.toString)
    verifyEvenDistribution(keys)
  }

  def verifyEvenDistribution(keys: Seq[HKey]) = {
    val histogram = new scala.collection.mutable.HashMap[Int, Int]()
    keys.foreach(key => {
      val partition = partitioner.getPartition(key)
      if (histogram.contains(partition)) histogram += (partition -> (histogram(partition) + 1)) else histogram += (partition -> 1)
    })
    val mean = histogram.map(_._2).reduce(_ + _).toDouble / histogram.size
    var stdev = math.sqrt(histogram.map(a => (math.pow(a._2, 2))).reduce(_ + _) / histogram.size - math.pow(mean, 2))
    var rsd = stdev / mean
    histogram should have size numPartitions
    rsd should be < 2.0 // relative st.deviation should be less than 2%
  }
}
