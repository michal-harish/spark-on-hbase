package org.apache.spark.hbase.demo

import org.apache.spark.hbase.HKey$
import org.apache.spark.hbase.demo.HE
import org.apache.spark.hbase.testing.TestSparkContext
import org.scalatest.Matchers
import org.scalatest.FlatSpec

class ExpansionTest extends FlatSpec with Matchers {

  DEMO.init(new TestSparkContext("local"))
  import DEMO._
  try {
    val s6 = fromTextList(HE("S6", 0.7), context.textFile("src/test/resources/screen6/links-2014-dec"))

    val i = HKey("a3bbde1c-543b-45c3-aab4-1737d7d1ccb3")
    val in: POOL = context.parallelize(Seq((i, i)))

    val out = expand(s6, in)
    println("-------------------------------------------")
    in.map(_._1).collect.foreach(v => println(v.toString))
    println("===========================================")
    out.map(_._1).collect.foreach(v => println(v.toString))

    val expansion = out.count / in.count * 100
    println(s"EXPANSION = ${expansion}%")
    expansion should be(300)

    //TODO merge s6 with syncs net and compute combined expansion
  } finally {
    DEMO.context.stop
  }
}
