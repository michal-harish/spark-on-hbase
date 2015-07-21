package org.apache.spark.hbase.demo

import DEMO._
import org.apache.hadoop.hbase.HConstants
import org.apache.spark.hbase.HKey$
import org.apache.spark.hbase.demo.HE
import org.apache.spark.hbase.testing.TestSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import org.scalatest.FlatSpec

import scala.collection.immutable.SortedMap

/**
 * Created by mharis on 03/07/15.
    existing state:
    d1 -> (d2:s6:0.7)
    d2 -> (d1:s6:0.7)

    additional connections:
    d1 -> (d3:cw:0.8)

    expected result state :
    d1 -> (d2:s6:0.7, d3:cw:0.8)
    d2 -> (d1:s6:0.7, d3:cw:0.56)
    d3 -> (d1:cw:0.8, d2:cw:0.56)
 */
class HGraphNetBSPProbabilitiesTest extends FlatSpec with Matchers {

  DEMO.init(new TestSparkContext("local"))

  try {
    //fixture - hbase state
    class EmulatedLookup[T] extends DEMO.HBaseLookup[EDGES, T] {

      override def apply(cfr: CFR[EDGES], rightSideRdd: RDD[(HKey, (Option[EDGES], T))])
      : RDD[(HKey, (Option[EDGES], T))] = {
        rightSideRdd.map({ case (key, (leftSideValue, rightSideValue)) => {
          val lookup: Map[HKey, EDGES] = SortedMap(
            HKey("d", "1") -> Seq((HKey("d", "2"), HE("S6", 0.7, 1000000L)))
            , HKey("d", "2") -> Seq((HKey("d", "1"), HE("S6", 0.7, 1000000L)))
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
    val adj = DEMO.fromPairs((DEMO.context.parallelize(Array[(HKey, (HKey, HE))](
      (HKey("d", "1") ->(HKey("d", "3"), HE("CROSSWISE", 0.8, 2000000L)))
    ))))
    adj.collect should be(Array(
      HKey("d", "1") -> Seq(((HKey("d", "3")), HE("CROSSWISE", 0.8, 2000000L)))
      , HKey("d", "3") -> Seq(((HKey("d", "1")), HE("CROSSWISE", 0.8, 2000000L)))
    ))

    val j = new EmulatedLookup[(EDGES, EDGES)]
    val (update, stats, history) = HGraphTest2.incrementalNetBSP(adj, j, 4)
    val updateData = update.sortByKey().collect
    assert(history.size == 4)
    history.foreach(_.unpersist(true))
    stats.foreach({ case (superstep, stat) => {
      println(s"BSP ${superstep} ${stat.name} = ${stat.value}")
    }
    })

    val output: Array[(HKey, (EDGES, EDGES))] = updateData.map(x => (x._1, (x._2._1.sorted, x._2._2.sorted)))
    println("\n")
    output.foreach(println)
    output.length should be(3)
    output(0) should be(HKey("d", "1") ->(Seq(((HKey("d", "2")), HE("S6", 0.7, 1000000L))), Seq(((HKey("d", "3")), HE("CROSSWISE", 0.8, 2000000L)))))
    output(1) should be(HKey("d", "2") ->(Seq(((HKey("d", "1")), HE("S6", 0.7, 1000000L))), Seq(((HKey("d", "3")), HE("CROSSWISE", 0.7 * 0.8, 1000000L)))))
    output(2) should be(HKey("d", "3") ->(Seq(), Seq(((HKey("d", "1")), HE("CROSSWISE", 0.8, 2000000L)), (((HKey("d", "2")), HE("CROSSWISE", 0.7 * 0.8, 1000000L))))))

    //(2:d,(List((1:d,P=0.6980392156862745@S6/1000000)),List((3:d,P=0.5568627450980392@S6/2000000))))
    //(2:d,(List((1:d,P=0.6980392156862745@S6/1000000)),List((3:d,P=0.5568627450980392@CROSSWISE/1000000))))
  } finally {
    context.stop
  }
}
