package org.apache.spark.hbase

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.hbase.misc.HttpRDD

/**
 * Created by mharis on 14/08/15.
 */
object HttpRDDTest extends App {
  val conf = new SparkConf
  conf.setAppName("test")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  val rdd = sc.parallelize(Array(
    ("A", (1,1,2,2,2)),
    ("B", (2,2,3,3,3))
  ))

  HttpRDD(rdd)
}