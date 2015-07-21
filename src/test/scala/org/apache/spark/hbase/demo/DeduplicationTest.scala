package org.apache.spark.hbase.demo

import org.apache.spark.hbase.demo.{HE, HBAGraph}
import org.apache.spark.hbase.{RegionPartitioner, HKey$}
import org.apache.spark.hbase.testing.TestSparkContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers

/**
 * Created by mharis on 28/06/15.
 */

class DeduplicationTest extends FlatSpec with HBAGraph with Matchers {
  val partitionerInstance = new RegionPartitioner(4)

  override def partitioner: RegionPartitioner = partitionerInstance



  //test simple ordering - Seq[Vid]
  val s: Seq[HKey] = Seq(HKey("test", "3"), HKey("test", "1"), HKey("test", "2"))
  s.sorted should be(Seq(HKey("test", "1"), HKey("test", "2"), HKey("test", "3")))

  //test simple ordering - EDGES
  val e: EDGES = Seq((HKey("test", "3"), HE("test3",1.0)), (HKey("test", "1"), HE("test1",1.0)), (HKey("test", "2"), HE("test2",1.0)))
  e.sorted should be (
    Seq(
      (HKey("test", "1"), HE("test1",1.0)), (HKey("test", "2"), HE("test2",1.0)), (HKey("test", "3"), HE("test3",1.0))
    )
  )

  initHBase(new TestSparkContext("local"))
  try {
    val list = Array(
      HKey("test", "3") -> Seq((HKey("test", "2"), HE("test1", 0.9, 100L)), (HKey("test", "1"), HE("test1", 0.9, 1000L))).sorted,
      HKey("test", "3") -> Seq((HKey("test", "1"), HE("test2", 0.8, 100L)), (HKey("test", "2"), HE("test2", 1.0, 1000L))).sorted
    )
    val n: NETWORK = context.parallelize(list)

    deduplicate(n).collect.foreach(println)
    deduplicate(n).collect should be(Array(
      HKey("test", "3") -> Seq((HKey("test", "1"), HE("test1",0.9)), (HKey("test", "2"), HE("test2",1.0)))
    ))

    val n2 = fromList(HE("test1",1.0), context.parallelize(Array(
      Seq(HKey("test", "3"), HKey("test", "1"), HKey("test", "2"))
    )))
    n2.collect.foreach(println)
  } finally {
    context.stop
  }
}
