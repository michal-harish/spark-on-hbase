package org.apache.spark.hbase

import java.util.UUID

import org.apache.spark.hbase.keyspace.KeySpaceRegistry.KSREG
import org.apache.spark.hbase.keyspace._
import org.scalatest.{Matchers, FlatSpec}

import scala.util.Random

/**
 * Created by mharis on 23/07/15.
 */
class KeySpaceTest extends FlatSpec with Matchers {

  val random = new Random
  val numPartitions = 32

  implicit val TestKeySpaceReg: KSREG = Map(
    new KeySpaceLong("l").keyValue,
    new KeySpaceLongPositive("lp").keyValue,
    new KeySpaceUUID("u").keyValue
  )

  val partitioner = new RegionPartitioner[Key](numPartitions, new KeySerDe[Key] {
    override def bytesToKey = (bytes: Array[Byte]) => Key(bytes)
    override def keyToBytes = (key: Key) => key.bytes
  })

  behavior of "KeySpaceLong"
  it should "have even distribution when partitioned by RegionPartitioner" in {
    val keys: Seq[Key] = for (i <- (0 to 100000)) yield Key("l", random.nextLong.toString)
    verifyEvenDistribution(keys)
  }

  behavior of "KeySpaceLongPositive"
  it should "have even distribution when partitioned by RegionPartitioner" in {
    val keys: Seq[Key] = for (i <- (0 to 100000)) yield Key("lp", math.abs(random.nextLong).toString)
    verifyEvenDistribution(keys)
  }

  behavior of "KeySpaceUUID"
  it should "have even distribution when partitioned by RegionPartitioner" in {

    val keys: Seq[Key] = for (i <- (0 to 100000)) yield Key("u", UUID.randomUUID.toString)
    verifyEvenDistribution(keys)
  }

  def verifyEvenDistribution(keys: Seq[Key]) = {
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
