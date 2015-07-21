package org.apache.spark.hbase.demo

import DEMO._
import org.apache.spark.hbase.HKey$
import org.apache.spark.hbase.demo.HE
import org.apache.spark.hbase.testing.TestSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import scala.collection.immutable.SortedMap

/**
 * Created by mharis on 11/06/15.
 */
class HGraphNetBSPUnitTest extends FlatSpec with Matchers {

  //fixture - hbase state
  class EmulatedLookup[T] extends DEMO.HBaseLookup[EDGES, T] {

    override def apply(cfr: CFR[EDGES], rightSideRdd: RDD[(HKey, (Option[EDGES], T))])
    : RDD[(HKey, (Option[EDGES], T))] = {
      rightSideRdd.map({ case (key, (leftSideValue, rightSideValue)) => {
        val he = HE("test1", 1.0, 1000L)
        val lookup: Map[HKey, EDGES] = SortedMap(
          HKey("d", "1") -> Seq((HKey("r", "1"), he), (HKey("r", "2"), he))
            , HKey("r", "1") -> Seq((HKey("d", "1"), he), (HKey("r", "2"), he))
            , HKey("d", "2") -> Seq((HKey("r", "3"), he), (HKey("r", "4"), he))
            , HKey("r", "2") -> Seq((HKey("d", "1"), he), (HKey("r", "1"), he))
            , HKey("r", "3") -> Seq((HKey("d", "2"), he), (HKey("r", "4"), he))
            , HKey("r", "4") -> Seq((HKey("d", "2"), he), (HKey("r", "3"), he))
        )
        leftSideValue match {
          case Some(left) => (key, (leftSideValue, rightSideValue))
          case None => {
            println(s"MULTIGET ${key}")
            (key, (if (lookup.contains(key)) Some(lookup(key)) else Some(null.asInstanceOf[EDGES]), rightSideValue))
          }
        }
      }
      })
    }
  }

  val oldhe = HE("test1", 1.0, 1000L)
  val newhe = HE("test2", 1.0, 2000L)

  DEMO.init(new TestSparkContext("local"))
  try {
    //fixture - additional syncs
    val syncs: PAIRS = DEMO.context.parallelize(Array[(HKey, (HKey, HE))](
      (HKey("r", "5"), (HKey("d", "1"), newhe))
      , (HKey("r", "5"), (HKey("d", "2"), newhe))
    ))
    val adj = DEMO.fromPairs(syncs)
    adj.collect should be (Array(
      HKey("d", "1") -> Seq((HKey("r", "5"), newhe))
      , HKey("d", "2") -> Seq((HKey("r", "5"), newhe))
      , HKey("r", "5") -> Seq((HKey("d", "1"), newhe), (HKey("d", "2"), newhe))
    ))

    //test the simulated rightLookupJoin works before we test the algorithm
    new EmulatedLookup[Int].apply(null, context.parallelize(Array[(HKey, (Option[EDGES], Int))](
      HKey("r", "1") ->(None, 10)
      , HKey("d", "2") ->(None, 100)
      , HKey("d", "20") ->(Some(Seq((HKey("d", "100000"), newhe))), 1000)
    ))).sortByKey().collect should be(Array(
      HKey("r", "1") ->(Some(Seq((HKey("d", "1"), oldhe), (HKey("r", "2"), oldhe))), 10)
      , HKey("d", "2") ->(Some(Seq((HKey("r", "3"), oldhe), (HKey("r", "4"), oldhe))), 100)
      , HKey("d", "20") ->(Some(Seq((HKey("d", "100000"), newhe))), 1000)
    ))

    //test the algroithm
    val (update, stats, history) = HGraphTest2.incrementalNetBSP(adj, new EmulatedLookup[(EDGES, EDGES)], 5)
    val updateData = update.sortByKey().collect
    assert(history.size == 5)
    history.foreach(_.unpersist(true))
    stats.foreach({ case (superstep, stat) => {
      println(s"BSP ${superstep} ${stat.name} = ${stat.value}")
    }
    })

    val output: Array[(HKey, (EDGES, EDGES))] = updateData.map(x =>  (x._1, (x._2._1.sorted, x._2._2.sorted)))
    println("\n")
    output.foreach(println)

    output.length should be(7)
    output(0) should be((HKey("d", "1") ->(Seq((HKey("r", "1"), oldhe), (HKey("r", "2"), oldhe)), Seq((HKey("d", "2"), newhe), (HKey("r", "3"),newhe), (HKey("r", "4"),newhe), (HKey("r", "5"),newhe)))))
    output(1) should be((HKey("r", "1") ->(Seq((HKey("d", "1"), oldhe), (HKey("r", "2"), oldhe)), Seq((HKey("d", "2"),newhe), (HKey("r", "3"),newhe), (HKey("r", "4"),newhe), (HKey("r", "5"),newhe)))))
    output(2) should be((HKey("d", "2") ->(Seq((HKey("r", "3"), oldhe), (HKey("r", "4"), oldhe)), Seq((HKey("d", "1"),newhe), (HKey("r", "1"),newhe), (HKey("r", "2"),newhe), (HKey("r", "5"),newhe)))))
    output(3) should be((HKey("r", "2") ->(Seq((HKey("d", "1"), oldhe), (HKey("r", "1"), oldhe)), Seq((HKey("d", "2"),newhe), (HKey("r", "3"),newhe), (HKey("r", "4"),newhe), (HKey("r", "5"),newhe)))))
    output(4) should be((HKey("r", "3") ->(Seq((HKey("d", "2"), oldhe), (HKey("r", "4"), oldhe)), Seq((HKey("d", "1"),newhe), (HKey("r", "1"),newhe), (HKey("r", "2"),newhe), (HKey("r", "5"),newhe)))))
    output(5) should be((HKey("r", "4") ->(Seq((HKey("d", "2"), oldhe), (HKey("r", "3"), oldhe)), Seq((HKey("d", "1"),newhe), (HKey("r", "1"),newhe), (HKey("r", "2"),newhe), (HKey("r", "5"),newhe)))))
    output(6) should be((HKey("r", "5") ->(Seq(), Seq((HKey("d", "1"),newhe), (HKey("r", "1"),newhe), (HKey("d", "2"),newhe), (HKey("r", "2"),newhe), (HKey("r", "3"),newhe), (HKey("r", "4"),newhe)))))


  } finally {
    context.stop
  }

}
