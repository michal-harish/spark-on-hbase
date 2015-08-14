package org.apache.spark.hbase.misc

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.sys.process.Process

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
