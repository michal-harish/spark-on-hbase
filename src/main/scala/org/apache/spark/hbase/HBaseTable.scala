package org.apache.spark.hbase

import java.util
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, HFileOutputFormat2}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.spark.SerializableWritable
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by mharis on 27/07/15.
 * This class is for rdd-based mutations to the underlying hbase table.
 *
 * Once application has an instance of HBaseTable extension this is the summary of methods that can be invoked on that instance:
 *
 * .rdd .rdd(cf*) .rdd(keySpace, cf*)
 * .update
 * .increment
 * .delete
 * .join
 * .lookup
 * .bulkLoad
 * .bulkDelete
 */
abstract class HBaseTable[K](
        val hbaConf: Configuration,
        val tableNameAsString: String,
        val numberOfRegions: Int,
        cfDescriptors: HColumnDescriptor*) {

  val tableName = TableName.valueOf(tableNameAsString)
  val families: Seq[HColumnDescriptor] = cfDescriptors

  protected def keyToBytes: K => Array[Byte]
  protected def bytesToKey: Array[Byte] => K

  /**
   * CFR = ColumnFamilyReader - a generic type of a function that maps hbase result to T
   */
  type CFR[T] = Result => T

  /**
   * HBaseJoin is an abstract function that joins another RDD with this table's RDD representation
   * - several implementations are avialble for different proportions of left and right tables
   */
  abstract class HBaseJoin[L, R] extends Function2[CFR[L], RDD[(K, R)], RDD[(K, (L, R))]]

  /**
   * HBaseLookup is a further optimisation of HBaseJoin for highly-iterative usecases like graph algorithms
   * where the already looked-up values can be cached and only as the algorithm 'explores' the underlying table
   * the new values are looked up - again multiple variants are implemented below
   */
  abstract class HBaseLookup[L, R]
    extends Function2[CFR[L], RDD[(K, (Option[L], R))], RDD[(K, (Option[L], R))]]


  //val numberOfRegions: Int = getNumRegions(hbaConf, TableName.valueOf(tableNameAsString))
  implicit val partitioner = new RegionPartitioner(numberOfRegions)

  /**
   * bulk load operations for put and delete wihich generate directly HFiles
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

  final protected val multiget = (table: Table, rowKeys: Iterable[Array[Byte]], cf: Seq[Array[Byte]]) => {
    val multiGetList = new util.ArrayList[Get]()
    for (keyBytes <- rowKeys) {
      val get = new Get(keyBytes)
      //TODO TIMELINE consistency -> hbase.region.replica.replication.enabled
      get.setConsistency(Consistency.STRONG)
      cf.foreach(get.addFamily(_))
      multiGetList.add(get)
    }
    table.get(multiGetList)
  }: Array[Result]

  def dropIfExists {
    val connection = ConnectionFactory.createConnection(hbaConf)
    val admin = connection.getAdmin
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

  def dropColumnIfExists(family: Array[Byte]): Boolean = {
    val connection = ConnectionFactory.createConnection(hbaConf)
    val admin = connection.getAdmin
    try {
      val columns = admin.getTableDescriptor(tableName).getColumnFamilies
      val cf = Bytes.toString(family)
      if (columns.exists(_.getNameAsString == cf)) {
        println(s"DROPPING COLUMN ${cf} FROM TABLE ${tableNameAsString}")
        admin.deleteColumn(tableName, family)
        true
      } else {
        false
      }
    } finally {
      admin.close
      connection.close
    }
  }

  final def createIfNotExists: Boolean = {
    val connection = ConnectionFactory.createConnection(hbaConf)
    val admin = connection.getAdmin
    try {
      if (!admin.tableExists(tableName)) {
        println("CREATING TABLE " + tableNameAsString)
        families.foreach(family => println(s"WITH COLUMN ${family.getNameAsString} TO TABLE ${tableNameAsString}"))
        val descriptor = new HTableDescriptor(tableName)
        families.foreach(f => descriptor.addFamily(f))
        admin.createTable(descriptor, partitioner.startKey, partitioner.endKey, numberOfRegions)
        return true
      } else {
        //alter columns
        val existingColumns = admin.getTableDescriptor(tableName).getColumnFamilies
        families.map(f => {
          if (!families.exists(_.getNameAsString == f.getNameAsString)) {
            println(s"ADDING COLUMN ${f.getNameAsString} TO TABLE ${tableNameAsString}")
            admin.addColumn(tableName, f)
            true
          } else if (f.compareTo(families.filter(_.getNameAsString == f.getNameAsString).head) != 0) {
            println(s"MODIFYING COLUMN ${f.getNameAsString} TO TABLE ${tableNameAsString}")
            admin.modifyColumn(tableName, f)
            true
          } else {
            false
          }
        }).exists(_ == true)
      }
    } finally {
      admin.close
      connection.close
    }
  }


  def update(family: Array[Byte], updateRdd: RDD[(K, Map[Array[Byte], (Array[Byte], Long)])])(implicit tag: ClassTag[K]): Long = {
    val context = updateRdd.context
    val broadCastConf = new SerializableWritable(hbaConf)
    val tableNameAsString = this.tableNameAsString
    val updateCount = context.accumulator(0L, "HGraph Net Update Counter")
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
    val broadCastConf = new SerializableWritable(hbaConf)
    val tableNameAsString = this.tableNameAsString
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
    val context = deleteRdd.context
    val broadCastConf = new SerializableWritable(hbaConf)
    val tableNameAsString = this.tableNameAsString
    val toDeleteCount = context.accumulator(0L, "HGraph Net Delete Counter1")
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
  def load(family: Array[Byte], bulkRdd: RDD[(K, Map[Array[Byte], (Array[Byte], Long)])], completeAsync: Boolean): Long = {
    val context = bulkRdd.context
    val acc = context.accumulator(0L, s"HBATable ${tableName} load count")

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
    val context = deleteRdd.context
    val acc = context.accumulator(0L, s"HBATable ${tableName} delete count")
    val cfs = families.map(_.getName)
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
    val context = hFileRdd.context
    val conf = hbaConf
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

  final class HBaseJoinRangeScan[L, R: ClassTag](cf: Array[Byte]*) extends HBaseJoin[L, R] {
    def apply(cfr: CFR[L], rightSideRddWithSortedPartitions: RDD[(K, R)]): RDD[(K, (L, R))] = {
      val cf = this.cf
      val broadCastConf = new SerializableWritable(hbaConf)
      val tableNameAsString = HBaseTable.this.tableNameAsString
      rightSideRddWithSortedPartitions.mapPartitionsWithIndex[(K, (L, R))] { case (p, part) => {
        if (part.isEmpty) {
          Iterator.empty
        } else new Iterator[(K, (L, R))] {
          val connection = ConnectionFactory.createConnection(broadCastConf.value)
          val table = connection.getTable(TableName.valueOf(tableNameAsString))
          val scan = new Scan();
          cf.foreach(scan.addFamily(_));
          scan.setMaxVersions(1)
          scan.setConsistency(Consistency.STRONG)
          // TODO consistency TIMELINE enable on hbase master
          val forward = part.buffered
          var scanner: ResultScanner = null
          var current: Option[(K, (L, R))] = None
          var leftRow: Result = null

          private def nextLeftRow {
            val headRowKey = keyToBytes(forward.head._1)
            //open new scan each time the difference between last and next vid is too big
            if (scanner == null || (1000 < ByteUtils.asIntValue(headRowKey) - ByteUtils.asIntValue(leftRow.getRow))) {
              if (scanner != null) scanner.close
              scan.setStartRow(headRowKey)
              scanner = table.getScanner(scan)
            }
            leftRow = scanner.next
          }

          override def hasNext: Boolean = {
            val result = current match {
              case Some(c) if (c == null) => false
              case Some(c) => true
              case None => {
                if (leftRow == null) nextLeftRow
                while (leftRow != null && current.isEmpty && forward.hasNext) {
                  val rightRow = forward.head
                  Bytes.compareTo(leftRow.getRow, keyToBytes(rightRow._1)) match {
                    case cmp: Int if (cmp == 0) => {
                      current = Some((rightRow._1, (cfr(leftRow), rightRow._2)))
                      forward.next
                    }
                    case cmp: Int if (cmp < 0) => nextLeftRow
                    case cmp: Int if (cmp > 0) => forward.next
                  }
                }
                current.isDefined
              }
            }
            if (!result) {
              if (scanner != null) scanner.close
              table.close
              connection.close
            }
            result
          }

          override def next(): (K, (L, R)) = {
            if (current.isEmpty) throw new NoSuchElementException
            val result = current.get
            current = None
            result
          }

        }
      }
      }
    }
  }

  final class HBaseJoinMultiGet[L, R](val maxGetSize: Int, val cf: Array[Byte]*) extends HBaseJoin[L, R] {
    def apply(cfr: CFR[L], rightSideRdd: RDD[(K, R)]): RDD[(K, (L, R))] = {
      val cf = this.cf
      val broadCastConf = new SerializableWritable(hbaConf)
      val tableNameAsString = HBaseTable.this.tableNameAsString
      val multiGetSize = maxGetSize
      val multiget = HBaseTable.this.multiget
      rightSideRdd.mapPartitions(part => {
        val connection = ConnectionFactory.createConnection(broadCastConf.value)
        val table = connection.getTable(TableName.valueOf(tableNameAsString))
        new Iterator[(K, (L, R))] {
          var buffer: Option[Iterator[(K, (L, R))]] = None

          override def hasNext: Boolean = buffer match {
            case None => loadBufferedIterator
            case Some(b) => b.hasNext match {
              case true => true
              case false => loadBufferedIterator
            }
          }

          override def next(): (K, (L, R)) = buffer match {
            case Some(i) => i.next
            case None => loadBufferedIterator match {
              case true => buffer.get.next
              case false => throw new NoSuchElementException
            }
          }

          private def loadBufferedIterator: Boolean = {
            try {
              multiGetWithAtLeastOneResult match {
                case None => {
                  table.close
                  connection.close
                  false
                }
                case Some(iterator) => {
                  buffer = Some(iterator)
                  true
                }
              }
            } catch {
              case _: Throwable => {
                table.close
                connection.close
                false
              }
            }
          }

          private def multiGetWithAtLeastOneResult: Option[Iterator[(K, (L, R))]] = {
            while (part.hasNext) {
              val multiGetList = mutable.MutableList[Array[Byte]]()
              val bufferMap = scala.collection.mutable.Map[K, (L, R)]()
              while (multiGetList.size < multiGetSize && part.hasNext) {
                val (vid, rightSideValue) = part.next
                multiGetList += keyToBytes(vid)
                bufferMap(vid) = ((null.asInstanceOf[L], rightSideValue))
              }
              if (!multiGetList.isEmpty) {
                multiget(table, multiGetList, cf).foreach(row => {
                  if (!row.isEmpty) {
                    val key = bytesToKey(row.getRow)
                    bufferMap(key) = (cfr(row), bufferMap(key)._2)
                  }
                })
                val resultBufferMap = bufferMap.filter(_._2._1 != null)
                if (!resultBufferMap.isEmpty) {
                  return Some(resultBufferMap.iterator)
                }
              }
            }
            None
          }
        }
      }, preservesPartitioning = true)
    }
  }

  final class HBaseLookupMultiGet[L, R](val batchSize: Int, val cf: Array[Byte]*) extends HBaseLookup[L, R] {
    def apply(cfr: CFR[L], rightSideRdd: RDD[(K, (Option[L], R))]): RDD[(K, (Option[L], R))] = {
      val cf = this.cf
      val broadCastConf = new SerializableWritable(hbaConf)
      val tableNameAsString = HBaseTable.this.tableNameAsString
      val multiGetSize = batchSize
      val multiget = HBaseTable.this.multiget
      rightSideRdd.mapPartitions(part => {
        val connection = ConnectionFactory.createConnection(broadCastConf.value)
        val table = connection.getTable(TableName.valueOf(tableNameAsString))
        val forward: BufferedIterator[(K, (Option[L], R))] = part.buffered
        val emptyLeftSide = Some(null.asInstanceOf[L])

        new Iterator[(K, (Option[L], R))] {
          var buffer: Option[Iterator[(K, (Option[L], R))]] = None
          var bufferMap = scala.collection.mutable.Map[K, (Option[L], R)]()
          var multiGet = mutable.MutableList[Array[Byte]]()

          override def hasNext: Boolean = forward.hasNext || checkBuffer

          override def next(): (K, (Option[L], R)) = if (checkBuffer) buffer.get.next else forward.next

          private def checkBuffer: Boolean = {
            if (buffer.isDefined) {
              if (buffer.get.hasNext) {
                return true
              } else {
                buffer = None
                bufferMap = scala.collection.mutable.Map[K, (Option[L], R)]()
                multiGet = mutable.MutableList[Array[Byte]]()
              }
            }
            while (forward.hasNext && forward.head._2._1.isEmpty && multiGet.size < multiGetSize) {
              val (vid, (leftSideValue, rightSideValue)) = forward.next
              multiGet += keyToBytes(vid)
              bufferMap(vid) = ((emptyLeftSide, rightSideValue))
            }
            if (!forward.hasNext || bufferMap.size >= multiGetSize) {
              val multiGetList = new util.ArrayList[Get]()
              multiget(table, multiGet, cf).foreach(row => if (!row.isEmpty) {
                val key = bytesToKey(row.getRow)
                bufferMap(key) = (Some(cfr(row)), bufferMap(key)._2)
              })

              val i = bufferMap.iterator
              buffer = Some(i)
              if (!i.hasNext) {
                if (!forward.hasNext) {
                  table.close
                  connection.close
                }
                false
              } else {
                true
              }
            } else {
              if (!forward.hasNext) {
                table.close
                connection.close
              }
              false
            }
          }
        }
      }, preservesPartitioning = true)
    }
  }
}
