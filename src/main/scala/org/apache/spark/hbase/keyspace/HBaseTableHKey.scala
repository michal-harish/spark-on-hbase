package org.apache.spark.hbase.keyspace

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.keyspace.HKeySpaceRegistry.HKSREG
import org.apache.spark.hbase.{ByteUtils, HBaseTable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SerializableWritable, SparkContext}

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


class HBaseTableHKey(hbaConf: Configuration, tableNameAsString: String, numberOfRegions: Int, cfDescriptors: HColumnDescriptor*)
                    (implicit reg: HKSREG)
  extends HBaseTable[HKey](hbaConf, tableNameAsString, numberOfRegions, cfDescriptors:_*) {

  /**
   * CFR = ColumnFamilyReader - a generic type of a function that maps hbase result to T
   */
  type CFR[T] = Result => T

  override protected def keyToBytes = (key: HKey) => key.bytes

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

  def rdd()(implicit context: SparkContext): RDD[(HKey, Result)] = new HBaseRddHKey(context, tableName)

  def rdd(cf: Array[Byte], maxStamp: Long)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRddHKey(context, tableName, HConstants.OLDEST_TIMESTAMP, maxStamp, Bytes.toString(cf))
  }

  def rdd(columns: String*)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRddHKey(context, tableName, columns: _*)
  }

  def rdd(keyIdSpace: Short, columns: String*)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRddHKey(context, tableName, keyIdSpace, columns: _*)
  }

  def rdd(keyIdSpace: Short, cf: Array[Byte], maxStamp: Long)(implicit context: SparkContext): RDD[(HKey, Result)] = {
    new HBaseRddHKey(context, tableName, keyIdSpace, HConstants.OLDEST_TIMESTAMP, maxStamp, Bytes.toString(cf))
  }

  final def get(vid: HKey*): Array[Result] = {
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

  final class HBaseJoinRangeScan[L, R: ClassTag](cf: Array[Byte]*) extends HBaseJoin[L, R] {
    def apply(cfr: CFR[L], rightSideRddWithSortedPartitions: RDD[(HKey, R)]): RDD[(HKey, (L, R))] = {
      val cf = this.cf
      val broadCastConf = new SerializableWritable(hbaConf)
      val tableNameAsString = HBaseTableHKey.this.tableNameAsString
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
      val tableNameAsString = HBaseTableHKey.this.tableNameAsString
      val multiGetSize = maxGetSize
      val multiget = HBaseTableHKey.this.multiget
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
      val tableNameAsString = HBaseTableHKey.this.tableNameAsString
      val multiGetSize = batchSize
      val multiget = HBaseTableHKey.this.multiget
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

}
