package org.apache.spark.hbase

import java.io.Serializable
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.HConstants._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{SerializableWritable, SparkContext}

import scala.reflect.ClassTag

/**
 * Created by mharis on 27/07/15.
 *
 * This class is for rdd-based mutations to the underlying hbase table.
 *
 * Once application has an instance of HBaseTable extension this is the summary of methods that can be invoked on that instance:
 *
 * .rdd
 * .rdd(cf*)
 * .rdd(minStamp, maxStamp, cf*)
 * .update
 * .increment
 * .delete
 * .bulkLoad
 * .bulkDelete
 */
abstract class HBaseTable[K](@transient protected val sc: SparkContext, val tableNameAsString: String) extends Serializable {

  type HBaseResultFunction[X] = Function[Result, X]

  @transient val hbaseConf: Configuration = Utils.initConfig(sc, HBaseConfiguration.create)

  @transient val tableName = TableName.valueOf(tableNameAsString)

  val numberOfRegions = Utils.getNumberOfRegions(hbaseConf, tableNameAsString)

  @transient val partitioner = new RegionPartitioner(numberOfRegions)

  def keyToBytes: K => Array[Byte]

  def bytesToKey: Array[Byte] => K

  type D = HBaseRDD[K, Result]

  def rdd: D = rdd(Consistency.STRONG, OLDEST_TIMESTAMP, LATEST_TIMESTAMP)

  def rdd(consistency: Consistency): D = rdd(consistency, OLDEST_TIMESTAMP, LATEST_TIMESTAMP)

  def rdd(columns: String*): D = rdd(Consistency.STRONG, OLDEST_TIMESTAMP, LATEST_TIMESTAMP, columns: _*)

  def rdd(consistency: Consistency, columns: String*): D = rdd(consistency, OLDEST_TIMESTAMP, LATEST_TIMESTAMP, columns: _*)

  def rdd(minStamp: Long, maxStamp: Long, columns: String*): D = rdd(Consistency.STRONG, minStamp, maxStamp, columns: _*)

  def rdd(consistency: Consistency, minStamp: Long, maxStamp: Long, columns: String*): D = {
    rdd[Result]((result: Result) => result, consistency, minStamp, maxStamp, columns: _*)
  }

  private def rdd[V](valueMapper: (Result) => V,
                     consistency: Consistency, minStamp: Long, maxStamp: Long, columns: String*): HBaseRDD[K, V] = {
    new HBaseRDD[K, V](sc, tableNameAsString, consistency, minStamp, maxStamp, null, columns: _*) {
      override def bytesToKey = HBaseTable.this.bytesToKey

      override def keyToBytes = HBaseTable.this.keyToBytes

      override def resultToValue = valueMapper
    }
  }

  def select(functions: Seq[HBaseFunction[_]])(implicit k: ClassTag[K]): HBaseRDD[K, Seq[_]] = {
    this.rdd(functions.flatMap(_.cols): _*).mapValues(result => functions.map(_(result)))
  }

  def select[F1: ClassTag](f1: HBaseFunction[F1])(implicit k: ClassTag[K]): HBaseRDD[K, F1] = {
    this.rdd(f1.cols: _*).mapValues(result => f1(result))
  }

  def select[F1: ClassTag, F2: ClassTag](f1: HBaseFunction[F1], f2: HBaseFunction[F2])
                                        (implicit k: ClassTag[K]): HBaseRDD[K, (F1, F2)] = {
    this.rdd(f1.cols ++ f2.cols: _*).mapValues(result => (f1(result), f2(result)))
  }

  def select[F1: ClassTag, F2: ClassTag, F3: ClassTag](
                                                        f1: HBaseFunction[F1],
                                                        f2: HBaseFunction[F2],
                                                        f3: HBaseFunction[F3]
                                                        )(implicit k: ClassTag[K]): HBaseRDD[K, (F1, F2, F3)] = {
    this.rdd(f1.cols ++ f2.cols ++ f3.cols: _*).mapValues(result => (f1(result), f2(result), f3(result)))
  }

  def select[F1: ClassTag, F2: ClassTag, F3: ClassTag, F4: ClassTag](
                                                                      f1: HBaseFunction[F1],
                                                                      f2: HBaseFunction[F2],
                                                                      f3: HBaseFunction[F3],
                                                                      f4: HBaseFunction[F4]
                                                                      )(implicit k: ClassTag[K]): HBaseRDD[K, (F1, F2, F3, F4)] = {
    this.rdd(f1.cols ++ f2.cols ++ f3.cols ++ f4.cols: _*).mapValues(
      result => (f1(result), f2(result), f3(result), f4(result)))
  }

  def update[V](f: HBaseFunction[V], u: RDD[(K,V)]) = {
    val broadCastConf = new SerializableWritable(hbaseConf)
    val tableNameAsString = this.tableNameAsString
    //f.applyInverse()
  }


  def put(family: Array[Byte], updateRdd: RDD[(K, Map[Array[Byte], (Array[Byte], Long)])])(implicit tag: ClassTag[K]): Long = {
    val broadCastConf = new SerializableWritable(hbaseConf)
    val tableNameAsString = this.tableNameAsString
    val updateCount = sc.accumulator(0L, "HGraph Net Update Counter")
    println(s"HBATable ${tableNameAsString} UPDATE RDD PARTITIONED BY ${updateRdd.partitioner}")
    updateRdd.partitionBy(partitioner).foreachPartition(part => {
      val connection = ConnectionFactory.createConnection(broadCastConf.value)
      try {
        val table = connection.getBufferedMutator(TableName.valueOf(tableNameAsString))
        var partCount = 0L
        part.foreach {
          case (key, cells) => if (cells.size > 0) {
            val put = new Put(keyToBytes(key))
            put.setDurability(Durability.SKIP_WAL)
            cells.foreach({
              case (qualifier, (value, timestamp)) => {
                put.addColumn(family, qualifier, timestamp, value)
              }
            })
            table.mutate(put)
            partCount += 1
          }
        }
        updateCount += partCount
        table.flush
        table.close
      } finally {
        connection.close
      }
    })
    updateCount.value
  }

  def increment(family: Array[Byte], qualifier: Array[Byte], incrementRdd: RDD[(K, Long)])(implicit tag: ClassTag[K]) {
    val broadCastConf = new SerializableWritable(hbaseConf)
    val tableNameAsString = this.tableNameAsString
    val keyToBytes = this.keyToBytes
    incrementRdd.partitionBy(partitioner).foreachPartition(part => {
      val connection = ConnectionFactory.createConnection(broadCastConf.value)
      val table = connection.getTable(TableName.valueOf(tableNameAsString))
      try {
        part.foreach {
          case (key, valueToAdd) => if (valueToAdd != 0) {
            val increment = new Increment(keyToBytes(key))
            increment.setDurability(Durability.SKIP_WAL)
            increment.addColumn(family, qualifier, valueToAdd)
            (null.asInstanceOf[ImmutableBytesWritable], increment)
            table.increment(increment)
          }
        }
      } finally {
        table.close
        connection.close
      }
    })
  }

  def delete(family: Array[Byte], deleteRdd: RDD[(K, Seq[Array[Byte]])])(implicit tag: ClassTag[K]): Long = {
    val broadCastConf = new SerializableWritable(hbaseConf)
    val tableNameAsString = this.tableNameAsString
    val toDeleteCount = sc.accumulator(0L, "HGraph Net Delete Counter1")
    val keyToBytes = this.keyToBytes
    deleteRdd.partitionBy(partitioner).foreachPartition(part => {
      val connection = ConnectionFactory.createConnection(broadCastConf.value)
      val mutator = connection.getBufferedMutator(TableName.valueOf(tableNameAsString))
      try {
        part.foreach { case (key, qualifiersToDelete) => {
          val delete = new Delete(keyToBytes(key))
          delete.setDurability(Durability.SKIP_WAL)
          if (qualifiersToDelete.contains(null)) {
            //nothing, just delete the whole row
          } else if (qualifiersToDelete.length == 0) {
            delete.addFamily(family)
          } else {
            qualifiersToDelete.foreach(qualifierToDelete => {
              delete.addColumn(family, qualifierToDelete)
            })
          }
          mutator.mutate(delete)
          toDeleteCount += 1
        }
        }
      } finally {
        mutator.close
        connection.close
      }
    })
    toDeleteCount.value
  }

  /**
   * bulk load operations for put and delete which generate directly HFiles
   * - requires the job/shell to be run us hbase user
   * - completeAsync - tells the job to complete the incremental load in the background or do it synchronously
   */
  val KeyValueOrdering = new Ordering[KeyValue] {
    override def compare(a: KeyValue, b: KeyValue) = {
      Bytes.compareTo(a.getRowArray, a.getRowOffset, a.getRowLength, b.getRowArray, b.getRowOffset, b.getRowLength) match {
        case 0 => Bytes.compareTo(a.getFamilyArray, a.getFamilyOffset, a.getFamilyLength, b.getFamilyArray, b.getFamilyOffset, b.getFamilyLength) match {
          case 0 => Bytes.compareTo(a.getQualifierArray, a.getQualifierOffset, a.getQualifierLength, b.getQualifierArray, b.getQualifierOffset, b.getQualifierLength) match {
            case 0 => if (a.getTimestamp < b.getTimestamp) 1 else if (a.getTimestamp > b.getTimestamp) -1 else 0
            case c: Int => c
          }
          case c: Int => c
        }
        case c: Int => c
      }
    }
  }

  def bulkLoad(family: Array[Byte], bulkRdd: RDD[(K, Map[Array[Byte], (Array[Byte], Long)])], completeAsync: Boolean): Long = {
    val acc = sc.accumulator(0L, s"HBATable ${tableName} load count")
    val keyToBytes = this.keyToBytes
    implicit val keyValueOrdering = KeyValueOrdering

    val hfileRdd: RDD[(ImmutableBytesWritable, KeyValue)] = bulkRdd.flatMap { case (key, columnVersions) => {
      val keyBytes = keyToBytes(key)
      columnVersions.map { case (qualifier, (value, timestamp)) => {
        (new KeyValue(keyBytes, family, qualifier, timestamp, KeyValue.Type.Put, value), keyBytes)
      }
      }
    }
    }.repartitionAndSortWithinPartitions(partitioner).map { case (keyValue, rowKey) => {
      acc += 1L
      (new ImmutableBytesWritable(rowKey), keyValue)
    }
    }.setName(s"HFileRDD PUT (${tableName})")

    bulk(hfileRdd, completeAsync)
    acc.value
  }

  def bulkDelete(family: Array[Byte], deleteRdd: RDD[(K, Seq[Array[Byte]])], completeAsync: Boolean): Long = {
    val acc = sc.accumulator(0L, s"HBATable ${tableName} delete count")
    val cfs = Utils.getColumnFamilies(hbaseConf, tableNameAsString).map(_.getName)
    val keyToBytes = this.keyToBytes
    implicit val keyValueOrdering = KeyValueOrdering

    val hFileRdd: RDD[(ImmutableBytesWritable, KeyValue)] = deleteRdd.flatMap { case (key, qualifiersToDelete) => {
      val keyBytes = keyToBytes(key)
      if (qualifiersToDelete.contains(null)) cfs.map(cf => {
        (new KeyValue(keyBytes, cf, null, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily), keyBytes)
      })
      else if (qualifiersToDelete.length == 0) {
        Seq((new KeyValue(keyBytes, family, null, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily), keyBytes))
      } else qualifiersToDelete.map(qualifier => {
        (new KeyValue(keyBytes, family, qualifier, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn), keyBytes)
      })
    }
    }.repartitionAndSortWithinPartitions(partitioner).map { case (keyValue, rowKey) => {
      acc += 1L
      (new ImmutableBytesWritable(rowKey), keyValue)
    }
    }.setName(s"HFileRDD DELETE (${tableName})")

    bulk(hFileRdd, completeAsync)
    acc.value
  }

  protected[spark] def bulk(hFileRdd: RDD[(ImmutableBytesWritable, KeyValue)], completeAsync: Boolean) = {
    val conf = hbaseConf
    val fs = FileSystem.get(conf)
    val hFile = new Path("/tmp", s"${tableNameAsString}_${UUID.randomUUID}")
    fs.makeQualified(hFile)
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    val connection = ConnectionFactory.createConnection(conf)
    try {
      val table = connection.getTable(tableName)
      val admin = connection.getAdmin
      val regionLocator = connection.getRegionLocator(tableName)
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

      hFileRdd.saveAsNewAPIHadoopFile(
        hFile.toString,
        classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2],
        job.getConfiguration)

      verifyFileStatus(fs, fs.getFileStatus(hFile), "hbase")

      val task = new Runnable() {
        override def run() = {
          try {
            val incrementalHFileLoader = new LoadIncrementalHFiles(conf)
            incrementalHFileLoader.doBulkLoad(hFile, admin, table, regionLocator)
            println(s"TASK LoadIncrementalHFiles (${hFile}) INTO ${hFileRdd.name} COMPLETED SUCCESFULLY")
          } catch {
            case e: Throwable => {
              println(s"TASK LoadIncrementalHFiles (${hFile}) INTO ${hFileRdd.name} FAILED")
              throw e
            }
          } finally {
            admin.close
            table.close
            regionLocator.close
            connection.close
            fs.deleteOnExit(hFile)
            fs.deleteOnExit(new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration)))
          }
        }
      }
      //TODO async task management by abstraction, e.g. can't exist job/shell if async tasks running, list tasks, etc..
      if (completeAsync) {
        println(s"LAUNCHING ASYNC TASK LoadIncrementalHFiles (${hFile})")
        new Thread(task).start()
      } else {
        println(s"LAUNCHING TASK LoadIncrementalHFiles (${hFile})")
        task.run()
      }
    } catch {
      case e: Throwable => {
        connection.close
        fs.deleteOnExit(hFile)
        throw e
      }
    }
  }


  /**
   * When using bulk operations the shell/job must be run as hbase user unfortunately
   */
  final def verifyFileStatus(fs: FileSystem, f: FileStatus, verifyOwner: String): Unit = {
    if (f.isDirectory) fs.listStatus(f.getPath).foreach(f1 => verifyFileStatus(fs, f1, verifyOwner))
    if (f.getOwner != verifyOwner) throw new IllegalStateException(s"This job must be run as `${verifyOwner}` user")
  }

}
