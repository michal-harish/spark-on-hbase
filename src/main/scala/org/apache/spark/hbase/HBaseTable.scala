package org.apache.spark.hbase

import java.util
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, HFileOutputFormat2, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.spark.{SparkContext, Accumulator, SerializableWritable}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by mharis on 21/07/15.
 *
 * This class is for rdd-based mutations to the underlying hbase table.
 * It can be instantiated directly with passed column family descriptors but ideally should be extended and
 * the underlying table should have region pre-split and managed manually as the whole implementation is built
 * around understanding the KeySpace distribution.
 *
 * Once application has an instance of HBaseTable this is the summary of methods that can be invoked on that instance:
 *
 * .rdd .rdd(cf*) .rdd(keySpace, cf*)
 * .update
 * .increment
 * .delete
 * .join
 * .lookup
 * .bulkLoad
 * .bulkDelete
 *
 * val h = new HBaseTable(conf, "my-hbase-table", 200,
 */


class HBaseTable(val hbaConf: Configuration,
                 val tableNameAsString: String,
                 val numberOfRegions: Int,
                 cfDescriptors: HColumnDescriptor*) extends HBaseDescriptors {

  //val numberOfRegions: Int = getNumRegions(hbaConf, TableName.valueOf(tableNameAsString))


  /**
   * CFR = ColumnFamilyReader - a generic type of a function that maps hbase result to T
   */
  type CFR[T] = Result => T

  /**
   * HBaseJoin is an abstract function that joins another RDD with this table's RDD representation
   * - several implementations are avialble for different proportions of left and right tables
   */
  abstract class HBaseJoin[L, R]
    extends Function2[CFR[L], RDD[(HKey, R)], RDD[(HKey, (L, R))]]

  /**
   * HBaseLookup is a further optimisation of HBaseJoin for highly-iterative usecases like graph algorithms
   * where the already looked-up values can be cached and only as the algorithm 'explores' the underlying table
   * the new values are looked up - again multiple variants are implemented below
   */
  abstract class HBaseLookup[L, R]
    extends Function2[CFR[L], RDD[(HKey, (Option[L], R))], RDD[(HKey, (Option[L], R))]]

  val partitioner = new RegionPartitioner(numberOfRegions)

  val tableName = TableName.valueOf(tableNameAsString)
  val trace = new org.apache.htrace.Trace
  val families: Seq[HColumnDescriptor] = cfDescriptors

  def rdd()(implicit context: SparkContext): RDD[(HKey, Result)] = new HBaseRdd(context, hbaConf, tableName)

  def rdd(cf: Array[Byte], maxStamp: Long)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRdd(context, hbaConf, tableName, HConstants.OLDEST_TIMESTAMP, maxStamp)
  }

  def rdd(columns: String*)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRdd(context, hbaConf, tableName, columns.map(Bytes.toBytes(_)): _*)
  }

  def rdd(keyIdSpace: Short, columns: String*)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRdd(context, hbaConf, tableName, keyIdSpace, columns.map(Bytes.toBytes(_)): _*)
  }

  def rdd(keyIdSpace: Short, cf: Array[Byte], maxStamp: Long)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRdd(context, hbaConf, tableName, keyIdSpace, HConstants.OLDEST_TIMESTAMP, maxStamp)
  }


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


  final def multiget(vid: HKey*): Array[Result] = {
    val connection = ConnectionFactory.createConnection(hbaConf)
    try {
      val table = connection.getTable(TableName.valueOf(tableNameAsString))
      val result = multiget(table, vid.map(_.bytes), Seq())
      table.close
      result
    } finally {
      connection.close
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

  /**
   * bulk load operations for put and delete wihich generate directly HFiles
   * - requires the job/shell to be run us hbase user
   * - closeContextOnExit - is for jobs that are submit for only the purpose of loading data
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

  def load(family: Array[Byte], bulkRdd: RDD[(HKey, Map[Array[Byte], (Array[Byte], Long)])], closeContextOnExit: Boolean, completeAsync: Boolean): Long = {
    val context = bulkRdd.context
    val acc = context.accumulator(0L, s"HBATable ${tableName} load count")

    implicit val keyValueOrdering = KeyValueOrdering

    val hfileRdd: RDD[(ImmutableBytesWritable, KeyValue)] = bulkRdd.flatMap { case (vid, columnVersions) => {
      columnVersions.map { case (qualifier, (value, timestamp)) => {
        (new KeyValue(vid.bytes, family, qualifier, timestamp, KeyValue.Type.Put, value), vid.bytes)
      }
      }
    }
    }.repartitionAndSortWithinPartitions(partitioner).map { case (keyValue, rowKey) => {
      acc += 1L
      (new ImmutableBytesWritable(rowKey), keyValue)
    }
    }.setName(s"HFileRDD PUT (${tableName})")

    bulk(hfileRdd, closeContextOnExit, completeAsync)
    acc.value
  }

  def bulkDelete(family: Array[Byte], deleteRdd: RDD[(HKey, Seq[Array[Byte]])], closeContextOnExit: Boolean, completeAsync: Boolean): Long = {
    val context = deleteRdd.context
    val acc = context.accumulator(0L, s"HBATable ${tableName} delete count")
    val cfs = families.map(_.getName)
    implicit val keyValueOrdering = KeyValueOrdering

    val hFileRdd: RDD[(ImmutableBytesWritable, KeyValue)] = deleteRdd.flatMap { case (vid, qualifiersToDelete) => {
      if (qualifiersToDelete.contains(null)) cfs.map(cf => {
        (new KeyValue(vid.bytes, cf, null, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily), vid.bytes)
      })
      else if (qualifiersToDelete.length == 0) {
        Seq((new KeyValue(vid.bytes, family, null, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily), vid.bytes))
      } else qualifiersToDelete.map(qualifier => {
        (new KeyValue(vid.bytes, family, qualifier, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn), vid.bytes)
      })
    }
    }.repartitionAndSortWithinPartitions(partitioner).map { case (keyValue, rowKey) => {
      acc += 1L
      (new ImmutableBytesWritable(rowKey), keyValue)
    }
    }.setName(s"HFileRDD DELETE (${tableName})")

    bulk(hFileRdd, closeContextOnExit, completeAsync)
    acc.value
  }

  private[spark] def bulk(hFileRdd: RDD[(ImmutableBytesWritable, KeyValue)], closeContextOnExit: Boolean, completeAsync: Boolean) = {
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

      if (closeContextOnExit) context.stop

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

  def update(family: Array[Byte], updateRdd: RDD[(HKey, Map[Array[Byte], (Array[Byte], Long)])]): Long = {
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
          case (vid, cells) => if (cells.size > 0) {
            val put = new Put(vid.bytes)
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

  def increment(family: Array[Byte], qualifier: Array[Byte], incrementRdd: RDD[(HKey, Long)]) {
    val broadCastConf = new SerializableWritable(hbaConf)
    val tableNameAsString = this.tableNameAsString
    incrementRdd.partitionBy(partitioner).foreachPartition(part => {
      val connection = ConnectionFactory.createConnection(broadCastConf.value)
      val table = connection.getTable(TableName.valueOf(tableNameAsString))
      try {
        part.foreach {
          case (vid, valueToAdd) => if (valueToAdd != 0) {
            val increment = new Increment(vid.bytes)
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

  def delete(family: Array[Byte], deleteRdd: RDD[(HKey, Seq[Array[Byte]])]): Long = {
    val context = deleteRdd.context
    val broadCastConf = new SerializableWritable(hbaConf)
    val tableNameAsString = this.tableNameAsString
    val toDeleteCount = context.accumulator(0L, "HGraph Net Delete Counter1")
    deleteRdd.partitionBy(partitioner).foreachPartition(part => {
      val connection = ConnectionFactory.createConnection(broadCastConf.value)
      val mutator = connection.getBufferedMutator(TableName.valueOf(tableNameAsString))
      try {
        part.foreach { case (key, qualifiersToDelete) => {
          val delete = new Delete(key.bytes)
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

  final class HBaseJoinRangeScan[L, R: ClassTag](cf: Array[Byte]*) extends HBaseJoin[L, R] {
    def apply(cfr: CFR[L], rightSideRddWithSortedPartitions: RDD[(HKey, R)]): RDD[(HKey, (L, R))] = {
      val cf = this.cf
      val broadCastConf = new SerializableWritable(hbaConf)
      val tableNameAsString = HBaseTable.this.tableNameAsString
      rightSideRddWithSortedPartitions.mapPartitionsWithIndex[(HKey, (L, R))] { case (p, part) => {
        if (part.isEmpty) {
          Iterator.empty
        } else new Iterator[(HKey, (L, R))] {
          val connection = ConnectionFactory.createConnection(broadCastConf.value)
          val table = connection.getTable(TableName.valueOf(tableNameAsString))
          val scan = new Scan();
          cf.foreach(scan.addFamily(_));
          scan.setMaxVersions(1)
          scan.setConsistency(Consistency.STRONG)
          // TODO consistency TIMELINE enable on hbase master
          val forward = part.buffered
          var scanner: ResultScanner = null
          var current: Option[(HKey, (L, R))] = None
          var leftRow: Result = null

          private def nextLeftRow {
            //open new scan each time the difference between last and next vid is too big
            if (scanner == null || (1000 < ByteUtils.asIntValue(forward.head._1.bytes) - ByteUtils.asIntValue(leftRow.getRow))) {
              if (scanner != null) scanner.close
              scan.setStartRow(forward.head._1.bytes)
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
                  Bytes.compareTo(leftRow.getRow, rightRow._1.bytes) match {
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

          override def next(): (HKey, (L, R)) = {
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
    def apply(cfr: CFR[L], rightSideRdd: RDD[(HKey, R)]): RDD[(HKey, (L, R))] = {
      val cf = this.cf
      val broadCastConf = new SerializableWritable(hbaConf)
      val tableNameAsString = HBaseTable.this.tableNameAsString
      val multiGetSize = maxGetSize
      val multiget = HBaseTable.this.multiget
      rightSideRdd.mapPartitions(part => {
        val connection = ConnectionFactory.createConnection(broadCastConf.value)
        val table = connection.getTable(TableName.valueOf(tableNameAsString))
        new Iterator[(HKey, (L, R))] {
          var buffer: Option[Iterator[(HKey, (L, R))]] = None

          override def hasNext: Boolean = buffer match {
            case None => loadBufferedIterator
            case Some(b) => b.hasNext match {
              case true => true
              case false => loadBufferedIterator
            }
          }

          override def next(): (HKey, (L, R)) = buffer match {
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

          private def multiGetWithAtLeastOneResult: Option[Iterator[(HKey, (L, R))]] = {
            while (part.hasNext) {
              val multiGetList = mutable.MutableList[Array[Byte]]()
              val bufferMap = scala.collection.mutable.Map[HKey, (L, R)]()
              while (multiGetList.size < multiGetSize && part.hasNext) {
                val (vid, rightSideValue) = part.next
                multiGetList += vid.bytes
                bufferMap(vid) = ((null.asInstanceOf[L], rightSideValue))
              }
              if (!multiGetList.isEmpty) {
                multiget(table, multiGetList, cf).foreach(row => {
                  if (!row.isEmpty) {
                    val key = HKey(row.getRow)
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
    def apply(cfr: CFR[L], rightSideRdd: RDD[(HKey, (Option[L], R))]): RDD[(HKey, (Option[L], R))] = {
      val cf = this.cf
      val broadCastConf = new SerializableWritable(hbaConf)
      val tableNameAsString = HBaseTable.this.tableNameAsString
      val multiGetSize = batchSize
      val multiget = HBaseTable.this.multiget
      rightSideRdd.mapPartitions(part => {
        val connection = ConnectionFactory.createConnection(broadCastConf.value)
        val table = connection.getTable(TableName.valueOf(tableNameAsString))
        val forward: BufferedIterator[(HKey, (Option[L], R))] = part.buffered
        val emptyLeftSide = Some(null.asInstanceOf[L])

        new Iterator[(HKey, (Option[L], R))] {
          var buffer: Option[Iterator[(HKey, (Option[L], R))]] = None
          var bufferMap = scala.collection.mutable.Map[HKey, (Option[L], R)]()
          var multiGet = mutable.MutableList[Array[Byte]]()

          override def hasNext: Boolean = forward.hasNext || checkBuffer

          override def next(): (HKey, (Option[L], R)) = if (checkBuffer) buffer.get.next else forward.next

          private def checkBuffer: Boolean = {
            if (buffer.isDefined) {
              if (buffer.get.hasNext) {
                return true
              } else {
                buffer = None
                bufferMap = scala.collection.mutable.Map[HKey, (Option[L], R)]()
                multiGet = mutable.MutableList[Array[Byte]]()
              }
            }
            while (forward.hasNext && forward.head._2._1.isEmpty && multiGet.size < multiGetSize) {
              val (vid, (leftSideValue, rightSideValue)) = forward.next
              multiGet += vid.bytes
              bufferMap(vid) = ((emptyLeftSide, rightSideValue))
            }
            if (!forward.hasNext || bufferMap.size >= multiGetSize) {
              val multiGetList = new util.ArrayList[Get]()
              multiget(table, multiGet, cf).foreach(row => if (!row.isEmpty) {
                val key = HKey(row.getRow)
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

  /**
   * When using bulk operations the shell/job must be run as hbase user unfortunately
   */
  final def verifyFileStatus(fs: FileSystem, f: FileStatus, verifyOwner: String): Unit = {
    if (f.isDirectory) fs.listStatus(f.getPath).foreach(f1 => verifyFileStatus(fs, f1, verifyOwner))
    if (f.getOwner != verifyOwner) throw new IllegalStateException(s"This job must be run as `${verifyOwner}` user")
  }
}
