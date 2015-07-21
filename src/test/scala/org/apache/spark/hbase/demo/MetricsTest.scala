package org.apache.spark.hbase.demo

import org.apache.spark.hbase.HKey$
import org.apache.spark.hbase.demo.HE
import org.apache.spark.hbase.testing.TestSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import org.scalatest.FlatSpec

import scala.math._

class MetricsTest extends FlatSpec with Matchers {

  DEMO.init(new TestSparkContext("local"))
  try {
    testAcquisitionWaste
  } finally {
    DEMO.context.stop
  }

  final def testPool(textFile: RDD[String]) = textFile.map(line => (HKey(line.trim), HKey(line.trim)))

  final def testSpanEventProfile(textFile: RDD[String]) = textFile.map(line => (HKey(line.trim), (1L, 1L, 1L)))
    .reduceByKey(DEMO.partitioner, (a: (Long, Long, Long), b: (Long, Long, Long)) => (min(a._1, b._1), max(a._2, b._2), a._3 + b._3): (Long, Long, Long))

  final def testAcquisitionWaste = {
    val e = testSpanEventProfile(DEMO.context.textFile("src/test/resources/pageviews/"))
    val testQuizzed = testPool(DEMO.context.textFile("src/test/resources/quizzed"))
    testQuizzed.collect.foreach(println)
    println("-------------------------------------------")
    val all = testQuizzed.count
    val baseProfile = DEMO.profile(testQuizzed, e)
    baseProfile.collect.foreach(println)
    val inWaste = (all - baseProfile.count).toDouble / all
    println(s"all=${all}, base waste=${inWaste}")
    inWaste should be(1.0 / 3.0)
    println("===========================================")
    val g = DEMO.fromList(HE("test1", 1.0), DEMO.context.parallelize(Seq(
      Seq(
        HKey("331d4fae-7dec-11d0-a765-00a0c91e6bf6"),
        HKey("071d4fae-7dec-11d0-a765-00a0c91e6bf6"),
        HKey("061d4fae-7dec-11d0-a765-00a0c91e6bf6")))))
    g.collect.foreach(println)
    println("-------------------------------------------")
    val expandedProfile = DEMO.innerExpand(g, testQuizzed, e)
    expandedProfile.collect.foreach(println)
    val outWaste = (all - expandedProfile.count).toDouble / all
    println(s"all=${all}, expanded waste=${outWaste}")
    outWaste should be(0.0)
  }

}

