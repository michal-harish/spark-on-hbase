package org.apache.spark.hbase

import java.util.UUID

import org.apache.spark.hbase.keyspace.KeySpaceRegistry.KSREG
import org.apache.spark.hbase.keyspace._
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 09/06/15.
 */
class RegionPartitionerTest extends FlatSpec with Matchers {

  val p1 = new RegionPartitioner[Array[Byte]](5, null)
  println(s"NUM REGIONS ${p1.numRegions}")
  p1.splitKeys.map(_.mkString("|")).foreach(println)
  println(s"HBASE startKey ${p1.startKey.mkString("|")}")
  println(s"HBASE endKey ${p1.endKey.mkString("|")}")
  p1.getPartition(ByteUtils.parseUUID("00000000-0000-0000-0000-000000000000")) should be(0)
  p1.getPartition(ByteUtils.parseUUID("33333333-3333-3333-3333-333333333332")) should be(0)
  p1.getPartition(ByteUtils.parseUUID("33333333-3333-3333-3333-333333333333")) should be(1)
  p1.getPartition(ByteUtils.parseUUID("66666666-6666-6666-6666-666666666665")) should be(1)
  p1.getPartition(ByteUtils.parseUUID("66666666-6666-6666-6666-666666666666")) should be(2)
  p1.getPartition(ByteUtils.parseUUID("99999999-9999-9999-9999-999999999998")) should be(2)
  p1.getPartition(ByteUtils.parseUUID("99999999-9999-9999-9999-999999999999")) should be(3)
  p1.getPartition(ByteUtils.parseUUID("cccccccc-cccc-cccc-cccc-cccccccccccb")) should be(3)
  p1.getPartition(ByteUtils.parseUUID("cccccccc-cccc-cccc-cccc-cccccccccccc")) should be(4)
  p1.getPartition(ByteUtils.parseUUID("ffffffff-ffff-ffff-ffff-fffffffffffe")) should be(4)
  p1.getPartition(ByteUtils.parseUUID("ffffffff-ffff-ffff-ffff-ffffffffffff")) should be(4)

  implicit val TestKeySpaceReg: KSREG = Map(
    new KeySpaceUUID("test").keyValue,
    new KeySpaceString("d").keyValue
  )

  val numRegions = 512
  val p = new RegionPartitioner[Key](numRegions,  new KeySerDe[Key] {
    override def bytesToKey = (bytes: Array[Byte]) => Key(bytes)
    override def keyToBytes = (key: Key) => key.bytes
  })


  val v0 = Key("test", "f81d4fae-7dec-11d0-a765-00a0c91e6bf6")
  val d0 = Key("d", "CASEAS000000000")
  val d1 = Key("d", "CASEASfffffffff")

  p.getPartition(v0) should be(496)
  p.getPartition(v0.bytes) should be(496)
  p.getPartition(d0) should be(411)
  p.getPartition(d0.bytes) should be(411)
  p.getPartition(d1) should be(267)
  p.getPartition(d1.bytes) should be(267)


  val hist = new scala.collection.mutable.HashMap[Int, Int]()
  val n = 1000000
  println(s"TESTING DISTRIBUTION OF ${n} RANDOM KEYs ACROSS ${numRegions} REGIONS")
  val e = n.toDouble / numRegions
  println(s"EXPECTED MEAN COUNT PER PARTITON = ${e}")
  println(s"BEST POSSIBLE DISTRIBUTION RANGE = [${e},${e}]")
  println(s"BEST POSSIBLE DISTRIBUTION STDEV = 0")
  print("...")
  val t = System.currentTimeMillis()
  for (i <- (1 to n)) {
    val key = Key("test", UUID.randomUUID.toString)
    val part = p.getPartition(key)
    if (hist.contains(part)) hist += (part -> (hist(part) + 1)) else hist += (part -> 1)
  }
  println(s"${System.currentTimeMillis() - t} ms")
  println(s"ACTUAL PARTITION COUNT = ${hist.size}")
  val mean = hist.map(_._2).reduce(_ + _).toDouble / hist.size
  val min = hist.map(_._2).foldLeft(Int.MaxValue)((a, b) => if (b < a) b else a)
  val max = hist.map(_._2).foldLeft(Int.MinValue)((a, b) => if (b > a) b else a)

  //hist.foreach(println)
  println(s"ACTUAL MEAN COUNT PER PARTITION = ${mean}")
  println(s"ACTUAL DISTRIBUTION RANGE= [${min},${max}]")
  var stdev = math.sqrt(hist.map(a => (math.pow(a._2, 2))).reduce(_ + _).toDouble / hist.size - math.pow(mean, 2))
  println(s"ACTUAL DISTRIBUTION STDEV = ±${stdev}")
  val devAboutMean = math.round(stdev / mean * 10000.0) / 100.0
  println(s"ACTUAL DISTRIBUTION STDEV = ±${devAboutMean} %")

  mean should be(e)
  devAboutMean should be < 2.5

}
