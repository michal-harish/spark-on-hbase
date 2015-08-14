package org.apache.spark.hbase.misc

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Durability, Put}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.hbase.keyspace.Key
import org.apache.spark.hbase.{HBaseTable, RegionPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SerializableWritable, SparkContext}

import scala.reflect.ClassTag

/**
 * Created by mharis on 17/06/15.
 */
object HBaseAdminUtils {

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
    val partitioner = new RegionPartitioner[Array[Byte]](numRegions, null)
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


  def copy[K: ClassTag](sc: SparkContext, src: HBaseTable[K], dest: HBaseTable[K]) {
    val broadCastConf = new SerializableWritable(HBaseAdminUtils.initConfig(sc, HBaseConfiguration.create))
    val srcTableNameAsString = src.tableNameAsString
    val destTableNameAsString = dest.tableNameAsString
    val updateCount = sc.accumulator(0L, "HBaseTable copy utility counter")
    println(s"HBaseTable COPYING ${srcTableNameAsString} TO ${destTableNameAsString}")
    val srcTransformed = src.rdd.partitionBy(new RegionPartitioner(dest.numberOfRegions, dest))
    srcTransformed.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection(broadCastConf.value)
      val destTable = connection.getBufferedMutator(TableName.valueOf(destTableNameAsString))
      try {
        var partCount = 0L
        part.foreach {
          case (key, result) => {
            val scanner = result.cellScanner()
            val put = new Put(src.toBytes(key))
            put.setDurability(Durability.SKIP_WAL)
            while (scanner.advance) {
              val cell = scanner.current
              put.add(cell)
            }
            partCount += 1
            destTable.mutate(put)
          }
        }
        updateCount += partCount
      } finally {
        destTable.close
      }
    })
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

}
