package org.apache.spark.hbase.testing

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process.Process

class TestSparkContext(config: SparkConf) extends SparkContext(config) {
  def this(parallelism: Int = 4) = this({
    val conf = new SparkConf().setAppName("dxp-spark-test")
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

/* kafka7 streaming simulation

<kafka-dist>/bin/kafka-console-consumer.sh --zookeeper ... --topic ...| nc -lk 9999
 */
object KafkaProxy {
  var port = 9999

  def apply(kafkaVersion: String, topic: String, zkConnect: String) = {
    port -= 1
    new KafkaProxy(kafkaVersion, topic, zkConnect, port)
  }
}

class KafkaProxy(kafkaVersion: String, val topic: String, zkConnect: String, val localPort: Int) {
  val KAFKA_HOME = kafkaVersion match {
    case "0.7" => "/opt/kafka-0.7.2-incubating-src"
    case "0.8" => "/opt/kafka-0.8.1-incubating-src"
  }
  val consumer = Process(s"${KAFKA_HOME}/bin/kafka-console-consumer.sh --zookeeper ${zkConnect} --topic ${topic} ")
  val server = Process(s"nc -lk ${localPort}")
  val process = (consumer #| server).run

  def start(ssc: StreamingContext): DStream[String] = ssc.socketTextStream("localhost", localPort)

  def stop = process.destroy()
}
