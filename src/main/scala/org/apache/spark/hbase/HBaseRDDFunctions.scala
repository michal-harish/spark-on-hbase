package org.apache.spark.hbase

import java.util

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SerializableWritable, Partitioner}
import org.apache.spark.rdd.{RDD, PairRDDFunctions}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by mharis on 27/07/15.
 */

trait HBaseRDDTypes[K,V] {
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
  abstract class HBaseLookup[L, R] extends Function2[CFR[L], RDD[(K, (Option[L], R))], RDD[(K, (Option[L], R))]]
}

class HBaseRDDFunctions[K, V](self: HBaseRDD[K,V])(implicit vk: ClassTag[K], vt: ClassTag[V])
  extends PairRDDFunctions[K,V](self)
  with HBaseRDDTypes[K,V]
{

  //TODO make sure we understand what self.withScope does
  override def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = self.withScope {
    val j = if (multiGetSize == -1) new HBaseJoinRangeScan[V, W](self.cf:_*) else new HBaseJoinMultiGet[V, W](1000, self.cf:_*)
    j(self.resultToValue, other)
    //TODO if partitioner is not equal to the self.partitioner add .partitionBy
  }

  def lookup[W](other: RDD[(K, (Option[V], W))]) : RDD[(K, (Option[V], W))] = {
    val l = new HBaseLookupMultiGet[V, W](1000, self.cf:_*)
    l(self.resultToValue, other)
  }

  val multiGetSize = 1000 // TODO make multiGetSize configurable
  val multiget = (table: Table, rowKeys: Iterable[Array[Byte]], cf: Seq[Array[Byte]]) => {
    val multiGetList = new util.ArrayList[Get]()
    for (keyBytes <- rowKeys) {
      val get = new Get(keyBytes)
      get.setConsistency(Consistency.STRONG) //TODO TIMELINE consistency -> hbase.region.replica.replication.enabled
      cf.foreach(get.addFamily(_))
      multiGetList.add(get)
    }
    table.get(multiGetList)
  }: Array[Result]

  final class HBaseJoinMultiGet[L, R](val maxGetSize: Int, val cf: Array[Byte]*) extends HBaseJoin[L, R] {
    def apply(cfr: CFR[L], rightSideRdd: RDD[(K, R)]): RDD[(K, (L, R))] = {
      val cf = this.cf
      val broadCastConf = new SerializableWritable(self.hbaseConf)
      val tableNameAsString = self.tableNameAsString
      val multiGetSize = maxGetSize
      val multiget = HBaseRDDFunctions.this.multiget
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
                multiGetList += self.keyToBytes(vid)
                bufferMap(vid) = ((null.asInstanceOf[L], rightSideValue))
              }
              if (!multiGetList.isEmpty) {
                multiget(table, multiGetList, cf).foreach(row => {
                  if (!row.isEmpty) {
                    val key = self.bytesToKey(row.getRow)
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

  final class HBaseJoinRangeScan[L, R](cf: Array[Byte]*) extends HBaseJoin[L, R] {
    def apply(cfr: CFR[L], rightSideRddWithSortedPartitions: RDD[(K, R)]): RDD[(K, (L, R))] = {
      val cf = this.cf
      val broadCastConf = new SerializableWritable(self.hbaseConf)
      val tableNameAsString = self.tableNameAsString
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
            val headRowKey = self.keyToBytes(forward.head._1)
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
                  Bytes.compareTo(leftRow.getRow, self.keyToBytes(rightRow._1)) match {
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

  final class HBaseLookupMultiGet[L, R](val batchSize: Int, val cf: Array[Byte]*) extends HBaseLookup[L, R] {
    def apply(cfr: CFR[L], rightSideRdd: RDD[(K, (Option[L], R))]): RDD[(K, (Option[L], R))] = {
      val cf = this.cf
      val broadCastConf = new SerializableWritable(self.hbaseConf)
      val tableNameAsString = self.tableNameAsString
      val multiGetSize = batchSize
      val multiget = HBaseRDDFunctions.this.multiget
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
              multiGet += self.keyToBytes(vid)
              bufferMap(vid) = ((emptyLeftSide, rightSideValue))
            }
            if (!forward.hasNext || bufferMap.size >= multiGetSize) {
              val multiGetList = new util.ArrayList[Get]()
              multiget(table, multiGet, cf).foreach(row => if (!row.isEmpty) {
                val key = self.bytesToKey(row.getRow)
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
