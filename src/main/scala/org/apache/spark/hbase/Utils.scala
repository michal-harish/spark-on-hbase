package org.apache.spark.hbase

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HTableDescriptor, HBaseConfiguration, HColumnDescriptor, TableName}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.hbase.keyspace.HKey
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import scala.reflect.ClassTag

/**
 * Created by mharis on 17/06/15.
 */
object Utils {

  final def initConfig[T <: Configuration](sc: SparkContext, config: T, fs: FileStatus*): T = {
    if (fs.size == 0) {
      val localFs: FileSystem = FileSystem.getLocal(config)
      initConfig(sc, config, localFs.listStatus(new Path(s"file://${sc.getConf.get("spark.executorEnv.HADOOP_CONF_DIR")}")): _*)
      initConfig(sc, config, localFs.listStatus(new Path(s"file://${sc.getConf.get("spark.executorEnv.HBASE_CONF_DIR")}")): _*)
    } else fs.foreach { configFileStatus => {
      if (configFileStatus.getPath.getName.endsWith(".xml")) {
        config.addResource(configFileStatus.getPath)
      }
    }
    }
    config
  }

  def getColumnFamilies(hbaseConfig: Configuration, tableNameAsString: String): Seq[HColumnDescriptor] = {
    val tableName = TableName.valueOf(tableNameAsString)
    val connection = ConnectionFactory.createConnection(hbaseConfig)
    try {
      val admin = connection.getAdmin
      try {
        admin.getTableDescriptor(tableName).getColumnFamilies
      } finally {
        admin.close
      }
    } finally {
      connection.close
    }
  }

  def getNumberOfRegions(hbaseConfig: Configuration, tableNameAsString: String): Int = {
    val tableName = TableName.valueOf(tableNameAsString)
    val connection = ConnectionFactory.createConnection(hbaseConfig)
    try {
      val regionLocator = connection.getRegionLocator(tableName)
      try {
        regionLocator.getStartKeys.length
      } finally {
        regionLocator.close
      }
    } finally {
      connection.close
    }
  }

  def getRegionSplits(hbaseConf: Configuration, tableName: TableName) = {
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val regionLocator = connection.getRegionLocator(tableName)
    try {
      val keyRanges = regionLocator.getStartEndKeys
      keyRanges.getFirst.zipWithIndex.map { case (startKey, index) => {
        (startKey, keyRanges.getSecond()(index))
      }
      }
    } finally {
      regionLocator.close
      connection.close
    }
  }

  def column(familyName: String,
             inMemory: Boolean,
             ttlSeconds: Int,
             bt: BloomType,
             maxVersions: Int,
             compression: Algorithm = Algorithm.SNAPPY,
             blocksize: Int = 64 * 1024): HColumnDescriptor = {
    val family: HColumnDescriptor = new HColumnDescriptor(familyName)
    family.setBlockCacheEnabled(true)
    family.setCompressionType(compression)
    family.setMinVersions(0) // min version overrides the ttl behaviour, i.e. minVersions=1 will always keep the latest cell versions despite the ttl
    family.setMaxVersions(maxVersions)
    family.setTimeToLive(ttlSeconds)
    family.setInMemory(inMemory)
    family.setBloomFilterType(bt)
    family.setBlocksize(blocksize)
    family
  }

  def updateSchema(sc: SparkContext, tableNameAsString: String, numRegions: Int, families: HColumnDescriptor*): Boolean = {
    println(s"CHECKING TABLE `${tableNameAsString}` WITH ${families.size} COLUMN FAMILIES: " + families.map(_.getNameAsString).mkString(","))
    val hbaseConf = initConfig(sc, HBaseConfiguration.create)
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    val tableName = TableName.valueOf(tableNameAsString)
    val partitioner = new RegionPartitioner(numRegions)
    try {
      if (!admin.tableExists(tableName)) {
        println("CREATING TABLE " + tableNameAsString)
        families.foreach(family => println(s"WITH COLUMN ${family.getNameAsString} TO TABLE ${tableNameAsString}"))
        val descriptor = new HTableDescriptor(tableName)
        families.foreach(f => descriptor.addFamily(f))
        admin.createTable(descriptor, partitioner.startKey, partitioner.endKey, numRegions)
        return true
      } else {
        println("TABLE ALREADY EXISITS " + tableNameAsString)
        //alter columns
        val existingColumns = admin.getTableDescriptor(tableName).getColumnFamilies
        families.map(f => {
          if (!existingColumns.exists(_.getNameAsString == f.getNameAsString)) {
            println(s"ADDING COLUMN ${f.getNameAsString} TO TABLE ${tableNameAsString}")
            admin.addColumn(tableName, f)
            true
          } else if (f.compareTo(existingColumns.filter(_.getNameAsString == f.getNameAsString).head) != 0) {
            println(s"MODIFYING COLUMN ${f.getNameAsString} TO TABLE ${tableNameAsString}")
            admin.modifyColumn(tableName, f)
            true
          } else {
            println(s"COLUMN ${f.getNameAsString} IN TABLE ${tableNameAsString} UNALTERED")
            false
          }
        }).exists(_ == true)
      }
    } finally {
      admin.close
      connection.close
    }
  }

  def dropIfExists(sc: SparkContext, tableNameAsString: String) {
    val hbaseConf = initConfig(sc, HBaseConfiguration.create)
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    val tableName = TableName.valueOf(tableNameAsString)
    try {
      if (admin.tableExists(tableName)) {
        println("DISABLING TABLE " + tableNameAsString)
        admin.disableTable(tableName)
        println("DROPPING TABLE " + tableNameAsString)
        admin.deleteTable(tableName)
      }
    } finally {
      admin.close
      connection.close
    }
  }

  def dropColumnIfExists(table: HBaseTable[_], family: Array[Byte]): Boolean = {
    val hbaseConf = table.hbaseConf
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    try {
      val columns = admin.getTableDescriptor(table.tableName).getColumnFamilies
      val cf = Bytes.toString(family)
      if (columns.exists(_.getNameAsString == cf)) {
        println(s"DROPPING COLUMN ${cf} FROM TABLE ${table.tableNameAsString}")
        admin.deleteColumn(table.tableName, family)
        true
      } else {
        false
      }
    } finally {
      admin.close
      connection.close
    }
  }

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
  final def saveAsKryo[T](rdd: RDD[(HKey, T)], path: String) = {
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

  final def loadKryo[T: ClassTag](sc: SparkContext, path: String)(implicit ct: ClassTag[T]): RDD[(HKey, T)] = {
    val kryoSerializer = new KryoSerializer(sc.getConf)
    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable])
      .flatMap(x => {
      val kryo = kryoSerializer.newKryo()
      val input = new Input()
      input.setBuffer(x._2.getBytes)
      kryo.readObject(input, classOf[Array[(HKey, T)]])
    })
  }

}
