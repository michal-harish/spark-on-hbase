package org.apache.spark.hbase.demo

import org.apache.spark.hbase.{HKey, HKey$}
import org.apache.spark.hbase.demo.HE
import org.apache.spark.hbase.testing.TestSparkContext
import org.scalatest.Matchers
import org.scalatest.FlatSpec

class ValidationTest extends FlatSpec with Matchers {

  DEMO.init(new TestSparkContext("local"))
  import DEMO._
  try {
    val w: PAIRS = context.textFile("src/test/resources/waitrose")
      .map(_.split(","))
      .map(l => (HKey(l(0)), (HKey("w_id", l(1)), HE("test1", 1.0))))

    val validation: NETWORK = bsp(fromPairs(w))

    val modelled = fromTextList(HE("test1", 1.0), context.textFile("src/test/resources/household"))

    f1(modelled, validation) should be((4.0, 4.0, 2.0, 0.5, 2.0 / 3.0))

    val modelled2 = fromTextList(HE("test2", 1.0), context.textFile("src/test/resources/screen6/links-2015-feb"))

    f1(modelled2, validation) should be((2.0, 0.0, 2.0, 1.0, 0.5))

    val u = deduplicate(modelled ++ modelled2)

    f1(u, validation)
  } finally {
    context.stop
  }

}
