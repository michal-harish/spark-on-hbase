package org.apache.spark.hbase.misc

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.io.{NullWritable, BytesWritable}
import org.apache.spark.SparkContext
import org.apache.spark.hbase.keyspace.Key
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import scala.reflect.ClassTag

/**
 * Created by mharis on 14/08/15.
 */
object RDDUtils {

  def info(tag: String, rdd: RDD[_]): Unit = {
    println(s"${tag} ${rdd.name} PARTITIONER: ${rdd.partitioner}")
    println(s"${tag} ${rdd.name} NUM.PARTITIONS: ${rdd.partitions.size}")
  }

  /**
   * Distribution of data across rdd partitions
   */
  def distribution(rdd: RDD[_]): Array[Int] = {
    rdd.mapPartitions(part => Array(part.size).iterator, true).collect
  }

  def printDistribution(rdd: RDD[_]): Unit = {
    val hist = distribution(rdd)
    hist.foreach(println)

    val mean = hist.reduce(_ + _).toDouble / hist.size
    val min = hist.foldLeft(Int.MaxValue)((a, b) => if (b < a) b else a)
    val max = hist.foldLeft(Int.MinValue)((a, b) => if (b > a) b else a)
    var dev = math.sqrt(hist.map(a => (math.pow(a, 2))).reduce(_ + _) / hist.size - math.pow(mean, 2))
    val stdev = math.round(dev / mean * 10000.0) / 100.0

    println(s"MEAN COUNT PER PARTITION = ${mean}")
    println(s"DISTRIBUTION RANGE= [${min},${max}]")
    println(s"DISTRIBUTION STDEV = ±${dev}")
    println(s"DISTRIBUTION STDEV = ±${stdev} %")
  }

  /**
   * Although we set spark serializer to kryo, the saveAsObject still outputs java serialized objects so
   * we provide our own method to dump the graph in kryo.
   */
  final def saveAsKryo[T](rdd: RDD[(Key, T)], path: String) = {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)
    rdd.mapPartitions(partition => partition.grouped(1000).map(_.toArray))
      .map(splitArray => {
      val kryo = kryoSerializer.newKryo()
      val bao = new ByteArrayOutputStream()
      val output = kryoSerializer.newKryoOutput()
      output.setOutputStream(bao)
      kryo.writeObject(output, splitArray)
      output.close()
      val byteWritable = new BytesWritable(bao.toByteArray)
      (NullWritable.get(), byteWritable)
    }).saveAsSequenceFile(path)
  }

  final def loadKryo[T: ClassTag](sc: SparkContext, path: String)(implicit ct: ClassTag[T]): RDD[(Key, T)] = {
    val kryoSerializer = new KryoSerializer(sc.getConf)
    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable])
      .flatMap(x => {
      val kryo = kryoSerializer.newKryo()
      val input = new Input()
      input.setBuffer(x._2.getBytes)
      kryo.readObject(input, classOf[Array[(Key, T)]])
    })
  }

}
