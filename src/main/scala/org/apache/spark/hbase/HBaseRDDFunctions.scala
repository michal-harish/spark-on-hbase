package org.apache.spark.hbase

import java.util

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{NullComparator, CompareFilter, SingleColumnValueFilter, SkipFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SerializableWritable}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by mharis on 27/07/15.
 *
 * Functions available for HBaseRDD via implicit conversions and HBaseFunction which
 * is used as a mapper of hbase row results and is an extension of a Function with some
 * added characteristics.
 *
 * TODO make sure we understand what self.withScope does
 */

class HBaseRDDFunctions[K, V](self: HBaseRDD[K, V])(implicit vk: ClassTag[K], vt: ClassTag[V]) extends Serializable {

  def filter(f: TransformationFilter[_]) = new HBaseRDDFiltered[K, V](self, new HBaseFilter {
    override def configureQuery(query: HBaseQuery): Unit = query.addFilter(f.createFilter)
  })

  def filter(consistency: Consistency) = new HBaseRDDFiltered[K, V](self, new HBaseFilter() {
    override def configureQuery(query: HBaseQuery): Unit = {
      query.setConsistency(consistency)
    }
  })

  def filter(minStamp: Long, maxStamp: Long) = new HBaseRDDFiltered[K, V](self, new HBaseFilter() {
    override def configureQuery(query: HBaseQuery): Unit = {

      if (minStamp != HConstants.OLDEST_TIMESTAMP || maxStamp != HConstants.LATEST_TIMESTAMP) {
        query.setTimeRange(minStamp, maxStamp)
      }
    }
  })

  def select(columns: String*) = new HBaseRDDFiltered[K, V](self, new HBaseFilter {
    override def configureQuery(query: HBaseQuery): Unit = {
      if (columns.size > 0) {
        columns.foreach(_ match {
          case cf: String if (!cf.contains(':')) => query.addFamily(Bytes.toBytes(cf))
          case column: String => column.split(":").map(Bytes.toBytes(_)) match {
            case Array(cf, qualifier) => {
              query.addColumn(cf, qualifier)
              val f = new SingleColumnValueFilter(cf, qualifier, CompareFilter.CompareOp.NOT_EQUAL, new NullComparator())
              f.setFilterIfMissing(true)
              query.addFilter(f)
            }
          }
        })
      }
    }
  })

  def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = self.withScope {
    join(other).partitionBy(partitioner)
  }

  // TODO provide per-join options multieget size and type of HBaseJoin implementation
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = self.withScope {
    val j = if (multiGetSize == -1) {
      new HBaseJoinRangeScan[W]
    } else {
      new HBaseJoinMultiGet[W](1000)
    }
    j(self, other)
  }

  def rightOuterJoin[W: ClassTag](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], W))] = self.withScope {
    rightOuterJoin(other).partitionBy(partitioner)
  }

  def rightOuterJoin[W: ClassTag](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = self.withScope {
    val l = new HBaseLookupMultiGet[W](multiGetSize)
    l(self, other.mapValues(x => (None.asInstanceOf[Option[V]], x)))
      .mapValues { case (left, right) => (if (left.isDefined && left.get == null) None else left, right) }
  }

  def fill[W](other: RDD[(K, (Option[V], W))]): RDD[(K, (Option[V], W))] = fill(other)

  def fill[W](other: RDD[(K, (Option[V], W))], partitioner: Partitioner): RDD[(K, (Option[V], W))] = self.withScope {
    val l = new HBaseLookupMultiGet[W](multiGetSize)
    l(self, other).partitionBy(partitioner)
  }

  val multiGetSize = 1000 // TODO make multiGetSize configurable

  val multiget = (self: HBaseRDD[K, V], table: Table, rowKeys: Iterable[Array[Byte]]) => {
    val multiGetList = new util.ArrayList[Get]()
    for (keyBytes <- rowKeys) {
      val get = new Get(keyBytes)
      self.configureQuery(get)
      multiGetList.add(get)
    }
    table.get(multiGetList)
  }: Array[Result]

  /**
   * HBaseJoin is an abstract function that joins another RDD with this table's RDD representation
   * - several implementations are avialble for different proportions of left and right tables
   */
  abstract class HBaseJoin[W] extends Function2[HBaseRDD[K, V], RDD[(K, W)], RDD[(K, (V, W))]]

  final class HBaseJoinMultiGet[W](val maxGetSize: Int) extends HBaseJoin[W] {
    def apply(self: HBaseRDD[K, V], rightSideRdd: RDD[(K, W)]): RDD[(K, (V, W))] = {
      val broadCastConf = new SerializableWritable(self.hbaseConf)
      val tableNameAsString = self.tableNameAsString
      val multiGetSize = maxGetSize
      val multiget = HBaseRDDFunctions.this.multiget
      val keyToBytes = self.keyToBytes
      val bytesToKey = self.bytesToKey
      rightSideRdd.mapPartitions(part => {
        val connection = ConnectionFactory.createConnection(broadCastConf.value)
        val table = connection.getTable(TableName.valueOf(tableNameAsString))
        new Iterator[(K, (V, W))] {
          var buffer: Option[Iterator[(K, (V, W))]] = None

          override def hasNext: Boolean = buffer match {
            case None => loadBufferedIterator
            case Some(b) => b.hasNext match {
              case true => true
              case false => loadBufferedIterator
            }
          }

          override def next(): (K, (V, W)) = buffer match {
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

          private def multiGetWithAtLeastOneResult: Option[Iterator[(K, (V, W))]] = {
            while (part.hasNext) {
              val multiGetList = mutable.MutableList[Array[Byte]]()
              val bufferMap = scala.collection.mutable.Map[K, (V, W)]()
              while (multiGetList.size < multiGetSize && part.hasNext) {
                val (key, rightSideValue) = part.next
                multiGetList += keyToBytes(key)
                bufferMap(key) = ((null.asInstanceOf[V], rightSideValue))
              }
              if (!multiGetList.isEmpty) {
                multiget(self, table, multiGetList).foreach(row => {
                  if (!row.isEmpty) {
                    val key = bytesToKey(row.getRow)
                    bufferMap(key) = (self.resultToValue(row), bufferMap(key)._2)
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

  final class HBaseJoinRangeScan[W] extends HBaseJoin[W] {
    def apply(self: HBaseRDD[K, V], rightSideRddWithSortedPartitions: RDD[(K, W)]): RDD[(K, (V, W))] = {
      val broadCastConf = new SerializableWritable(self.hbaseConf)
      val tableNameAsString = self.tableNameAsString
      val keyToBytes = self.keyToBytes
      rightSideRddWithSortedPartitions.mapPartitionsWithIndex[(K, (V, W))] { case (p, part) => {
        if (part.isEmpty) {
          Iterator.empty
        } else new Iterator[(K, (V, W))] {
          val connection = ConnectionFactory.createConnection(broadCastConf.value)
          val table = connection.getTable(TableName.valueOf(tableNameAsString))
          val scan = new Scan()
          self.configureQuery(scan)
          val forward = part.buffered
          var scanner: ResultScanner = null
          var current: Option[(K, (V, W))] = None
          var leftRow: Result = null

          private def nextLeftRow {
            val headRowKey = keyToBytes(forward.head._1)
            //open new scan each time the difference between last and next row key is too big
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
                      current = Some((rightRow._1, (self.resultToValue(leftRow), rightRow._2)))
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

          override def next(): (K, (V, W)) = {
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

  /**
   * HBaseLookup is a further optimisation of HBaseJoin for highly-iterative usecases like graph algorithms
   * where the already looked-up values can be cached and only as the algorithm 'explores' the underlying table
   * the new values are looked up - again multiple variants are implemented below
   */
  abstract class HBaseLookup[W] extends Function2[HBaseRDD[K, V], RDD[(K, (Option[V], W))], RDD[(K, (Option[V], W))]]


  final class HBaseLookupMultiGet[W](val batchSize: Int) extends HBaseLookup[W] {
    def apply(self: HBaseRDD[K, V], rightSideRdd: RDD[(K, (Option[V], W))]): RDD[(K, (Option[V], W))] = {
      val broadCastConf = new SerializableWritable(self.hbaseConf)
      val tableNameAsString = self.tableNameAsString
      val multiGetSize = batchSize
      val multiget = HBaseRDDFunctions.this.multiget
      val keyToBytes = self.keyToBytes
      val bytesToKey = self.bytesToKey
      rightSideRdd.mapPartitions(part => {
        val connection = ConnectionFactory.createConnection(broadCastConf.value)
        val table = connection.getTable(TableName.valueOf(tableNameAsString))
        val forward: BufferedIterator[(K, (Option[V], W))] = part.buffered
        val emptyLeftSide = Some(null.asInstanceOf[V])

        new Iterator[(K, (Option[V], W))] {
          var buffer: Option[Iterator[(K, (Option[V], W))]] = None
          var bufferMap = scala.collection.mutable.Map[K, (Option[V], W)]()
          var multiGet = mutable.MutableList[Array[Byte]]()

          override def hasNext: Boolean = forward.hasNext || checkBuffer

          override def next(): (K, (Option[V], W)) = if (checkBuffer) buffer.get.next else forward.next

          private def checkBuffer: Boolean = {
            if (buffer.isDefined) {
              if (buffer.get.hasNext) {
                return true
              } else {
                buffer = None
                bufferMap = scala.collection.mutable.Map[K, (Option[V], W)]()
                multiGet = mutable.MutableList[Array[Byte]]()
              }
            }
            while (forward.hasNext && forward.head._2._1.isEmpty && multiGet.size < multiGetSize) {
              val (key, (leftSideValue, rightSideValue)) = forward.next
              multiGet += keyToBytes(key)
              bufferMap(key) = ((emptyLeftSide, rightSideValue))
            }
            if (!forward.hasNext || bufferMap.size >= multiGetSize) {
              val multiGetList = new util.ArrayList[Get]()
              multiget(self, table, multiGet).foreach(row => if (!row.isEmpty) {
                val key = bytesToKey(row.getRow)
                bufferMap(key) = (Some(self.resultToValue(row)), bufferMap(key)._2)
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
