package org.apache.spark.hbase

import org.apache.spark.hbase.testing.TestSparkContext
import org.scalatest._

class HKeySpaceLongPositiveTest extends FlatSpec with Matchers {
  val context = new TestSparkContext("local")

  import context._

  try {
    val numPartitions = 32
    val p = new RegionPartitioner(numPartitions)

    val ids = textFile("src/test/resources/appnexus/")

    ids.collect.foreach(id => id should be(HKey("a", id).asString))

    val hist = new scala.collection.mutable.HashMap[Int, Int]()

    val partitionedVids = ids
      .map(id => (HKey("a", id), "a"))
      .partitionBy(p)

    partitionedVids.collect.foreach { x =>
      val partition = p.getPartition(x._1)
      if (hist.contains(partition)) hist += (partition -> (hist(partition) + 1)) else hist += (partition -> 1)
    }

    hist should have size numPartitions

    val mean = hist.map(_._2).reduce(_ + _).toDouble / hist.size
    var stddev = math.sqrt(hist.map(a => (math.pow(a._2, 2))).reduce(_ + _) / hist.size - math.pow(mean, 2))

    mean shouldBe 31.25
    stddev.toInt should be <= (6)
  } finally {
    context.stop
  }

}