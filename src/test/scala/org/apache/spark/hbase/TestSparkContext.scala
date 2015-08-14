package org.apache.spark.hbase

import org.apache.spark.{SparkConf, SparkContext}

class TestSparkContext(config: SparkConf) extends SparkContext(config) {
  def this(parallelism: Int = 4) = this({
    val conf = new SparkConf().setAppName("spark-on-hbase-test")
    conf.setMaster(s"local[${parallelism}]")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.default.parallelism", s"${parallelism}")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.apache.spark.hbase.demo.KryoRegistrator")
    conf.set("spark.kryo.referenceTracking", "false")
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf
  })
}