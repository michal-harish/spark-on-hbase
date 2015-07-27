package org.apache.spark.hbase.demo

import org.apache.spark.hbase.RegionPartitioner
import org.apache.spark.hbase.keyspace.HKey
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.{rddToOrderedRDDFunctions, rddToPairRDDFunctions}
import org.apache.spark.storage.StorageLevel._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

trait Props[A <: Props[A]] {
  def combine(other: A): A;
}

/**
 * Custom implementation of a Property Graph
 */
trait AGraph[PROPS <: Props[PROPS]] {

  type HISTORY = scala.collection.mutable.MutableList[RDD[_]]

  /**
   * LAYER is an abstract concept which is represented by a Key-Value Spark RDD with fixed key type - Vid - and
   * unspecified value type.
   * A general Graph may consist of several layers and as long as the elements in each are keyed by the same type the
   * set of layers can be viewed and analysied as a single graph.
   */
  type LAYER[X] = RDD[(HKey, X)]

  /**
   * NETWORK is a LAYER which represents the structure of the graph. It is keyed by Vid (as any other LAYER) where each
   * value is a sequence of outgoing edges
   */

  type EDGE = (HKey, PROPS)
  type EDGES = Seq[EDGE]
  // TODO consider hashmap instead of sequence because of frequent .exists operations
  type NETWORK = LAYER[EDGES]

  type PAIRS = LAYER[EDGE]

  /**
   * POOL - is a list of IDs mapped to another list of IDs, for disconnected pool both Vid(s) in the tuple are same
   */
  type POOL = LAYER[HKey]

  def minimize(net: NETWORK, keySpace: Short): NETWORK = net.filter(_._1.keySpace == keySpace).mapValues(_.filter(_._1.keySpace == keySpace))

  def limit[N, C](net: LAYER[N], constraint: LAYER[C])(implicit t: ClassTag[N]): LAYER[N] = net.join(constraint).map(x => (x._1, x._2._1))

  /**
   * converts a network into a pool of pairs
   */
  def flatten(net: NETWORK): POOL = net.map { case (vid, edges) => (vid, HKey.highest(vid, edges.map(_._1))) }

  /**
   * generic histogram function
   */
  final def hist(layer: LAYER[Long]): Array[(Long, Long)] = layer.map(v => (v._2, 1L)).aggregateByKey(0L)(_ + _, _ + _).sortByKey().collect

  /**
   * histogram of number of outgoing edges per vertex - this is not an RDD but a sorted and collected sequence
   */
  final def histogram(network: NETWORK): Array[(Long, Long)] = hist(network.mapValues(_.size))

  final def frequency(pairs: PAIRS)(implicit partitioner: RegionPartitioner): LAYER[Long] = {
    reverse(pairs).aggregateByKey(0L, partitioner)((u, v) => u + 1L, (u1, u2) => u1 + u2)
  }

  /**
   * From undirected pairs creates a REDUNDANT directed network graph
   */
  final def fromPairs(in: PAIRS)(implicit partitioner: RegionPartitioner): NETWORK = group(reverse(in))

  /**
   * Reverse given pairs and double the footprint
   */
  final def reverse(in: PAIRS): PAIRS = {
    in.flatMap { case (src, (dest, props)) => Seq((src, (dest, props)), (dest, (src, props))) }
  }

  /**
   * this effectively performs a 1-step connected components BSP on an entire network -
   * not efficient as netBSP but useful for small validation sets and incremental batches
   */
  final def bsp(in: NETWORK)(implicit partitioner: RegionPartitioner): NETWORK = {
    deduplicate(in.flatMap { case (vid, edges) => {
      (for (edge <- edges) yield ((edge._1, edges.filter(_._1 != edge._1)))) :+(vid, edges)
    }
    })
  }

  /**
   * Removes extremely inter-connected groups built from a set of pairs
   */
  final def cutoff(pairs: PAIRS, cutOffThreshold: Double = 0.05)(implicit partitioner: RegionPartitioner): PAIRS = {
    val freq = frequency(pairs).persist(MEMORY_AND_DISK_SER)
    try {
      val h = hist(freq)
      var total = 0L
      val x = h.map { case (edges, count) => {
        total += edges * count; (edges, count, total)
      }
      }
      val maxEdges = x.filter(_._3 < total * (1.0 - cutOffThreshold)).last._1
      val extremes = freq.filter(_._2 > maxEdges).map(_._1).collect.toSet
      pairs.filter { case (l, (r, props)) => !extremes.contains(l) && !extremes.contains(r) }.setName(s"FILTERED ${pairs.name}")
    } finally {
      freq.unpersist(true)
    }
  }

  /*
   * group - deduplicate a network directly from PAIRS
   */
  final def group(pairs: PAIRS)(implicit partitioner: RegionPartitioner): NETWORK = deduplicate(pairs.mapValues(Seq(_)))

  /**
   * combines all occurrences of the same vertex id into a network of unique vertices with combined and deduplicated edges
   * merge-sort optimized implementation of (a ++ b).distinct.sortWith(Vid.comparator)
   * WARNING: because it is a merge sort, the individual edge sequences are expected to be already sorted
   * the input net RDD doesn't have to be sorted by key but the EDGES within each row MUST be sorted
   */
  final def deduplicate(net: NETWORK)(implicit partitioner: RegionPartitioner): NETWORK = {
    val mergeSortAscending = (g: Iterable[EDGES]) => {

      if (g.size == 1) {
        g.head
      } else {
        val result = Seq.newBuilder[EDGE]
        val i = ListBuffer[Int]()
        val len = ListBuffer[Int]()
        val seq = ListBuffer[EDGES]()
        g.foreach(group => {
          i += 0
          len += group.length
          seq += group
        })
        var resultLen = 0
        while ((len zip i).foldLeft(false)((a, b) => a || (b._2 < b._1))) {
          var selectedGroup: Int = -1
          var selected: EDGE = null
          for (group <- 0 to g.size - 1) if (i(group) < len(group)) {
            if (selectedGroup == -1) {
              selectedGroup = group
              selected = seq(selectedGroup)(i(selectedGroup))
            } else {
              var cmp = 0
              do {
                val inspected = seq(group)(i(group))
                cmp = inspected._1.compareTo(selected._1)
                if (cmp == 0) {
                  selected = (selected._1, selected._2.combine(inspected._2))
                  i(group) += 1
                  if (i(group) >= len(group)) {
                    cmp = 1
                  }
                }
              } while (cmp == 0)
              if (cmp < 0) {
                selectedGroup = group
                selected = seq(selectedGroup)(i(selectedGroup))
              }
            }
          }
          result += selected
          resultLen += 1
          i(selectedGroup) += 1
        }
        (result.result)
      }
    }: EDGES

    net.repartitionAndSortWithinPartitions(partitioner)
      .mapPartitions(part => {
      val forward = part.buffered
      new Iterator[(HKey, EDGES)] {
        var current: Option[(HKey, EDGES)] = None

        override def hasNext: Boolean = {
          current match {
            case Some(c) if (c == null) => false
            case Some(c) => true
            case None => loadAndMergeNextGroup
          }
        }

        override def next(): (HKey, EDGES) = {
          if (current.isEmpty) throw new NoSuchElementException
          val result = current.get
          current = None
          result
        }

        private def loadAndMergeNextGroup: Boolean = {
          if (forward.hasNext) {
            val vid: HKey = forward.head._1
            val group = mutable.MutableList[EDGES]()
            do {
              group += forward.head._2
              forward.next
            } while (forward.hasNext && forward.head._1 == vid)
            current = Some((vid, mergeSortAscending(group)))
          }
          current.isDefined
        }
      }
    })
  }

  /**
   * returns (vertex count, group count) of a collapsed graph
   */
  final def counts(net: NETWORK): (Long, Long) = {
    val counts: (Long, Double) = net.map(x => (1, (1.0 / (1.0 + x._2.length.toDouble))))
      .aggregate((0L, 0.0))((b, a) => (b._1 + a._1, b._2 + a._2), (b, c) => (b._1 + c._1, b._2 + c._2))
    println("COOKIES > USERS = " + counts._1 + " > " + counts._2.toLong + " = DEDUPLICATION = " + counts._2.toDouble / counts._1 * 100 + " %")
    (counts._1, counts._2.toLong)
  }

  final def count(pool: POOL): (Long, Long) = {
    val counts = pool.map { case (vid, maxVid) => (1L, if (vid.equals(maxVid)) 1L else 0L) }
      .aggregate((0L, 0L))((b, a) => (b._1 + a._1, b._2 + a._2), (b, c) => (b._1 + c._1, b._2 + c._2))
    println("COOKIES > USERS = " + counts._1 + " > " + counts._2 + " = DEDUPLICATION = " + counts._2.toDouble / counts._1 * 100 + " %")
    counts
  }

  /**
   * Simple EXPAND takes a pool of vertices and expands it through the given graph network
   * and returns an expanded pool of vertex pairs: in each 1. unique vertex and 2. highest connected vertex
   */
  final def expand(net: NETWORK, users: POOL)(implicit partitioner: RegionPartitioner): POOL = {
    users.leftOuterJoin(net, partitioner).flatMap({ case (key, (maxEdge, netEdges)) => {
      netEdges match {
        case Some(edges) if (edges.length > 0) => {
          val max = HKey.highest(key, edges.map(_._1))
          edges.map(edge => (edge._1, max)) :+(key, max)
        }
        case _ => Seq((key, maxEdge))
      }
    }
    }).reduceByKey(partitioner, (a, b) => HKey.higher(a, b))
  }

  final def profile[X](users: POOL, profile: LAYER[X])(implicit partitioner: RegionPartitioner): RDD[(HKey, (HKey, X))] = {
    users.join(profile, partitioner)
  }

  /**
   * expand - expands the pool of ids using the network's connectivity into a new list
   * and do a straight join on available profile
   */
  final def expand[X](net: NETWORK, pool: POOL, profile: LAYER[X])(implicit partitioner: RegionPartitioner): RDD[(HKey, (HKey, X))] = {
    expand(net, pool).join(profile, partitioner)
  }

  /**
   * innerExpand is similar to expand but goes further - like expand, it first expands the given pool
   * using the network's connectivity and combines it with the profile
   * but unlike the expand it then joins the expanded profile on the original unexpanded pool
   */
  final def innerExpand[X](net: NETWORK, pool: POOL, profile: LAYER[X] /*, combiner: (X,X) => X*/)
                          (implicit t: ClassTag[X], partitioner: RegionPartitioner): RDD[(HKey, (HKey, X))] = {
    val exp: POOL = expand(net, pool)
    exp.collect.foreach(x => println(s"EXPANDED POOL ${x}"))
    val expandedProfile = exp.join(profile, partitioner)
    /**/
    val invertedProfile = expandedProfile.map { case (vid, (maxVid, x)) => (maxVid, x) } //.reduceByKey( (a,b) => combiner(a,b))
    val invertedPool = exp.map { case (vid, maxVid) => (maxVid, vid) }
    invertedPool.join(invertedProfile, partitioner).map { case (maxVid, (vid, x)) => (vid, (maxVid, x)) }
      .join(pool, partitioner)
      .map { case (vid, (maxProfile, originalMaxVid)) => (vid, maxProfile) }
    //TODO pool.join(expandedProfile, partitioner).mapValues { case(maxVid, (maxVid2, x)) => (maxVid, x) }
  }

  /**
   * returns a tuple value aggregate of the overlay computed by profile or expand
   */
  final def aggregate[X: ClassTag](layer: LAYER[X], sum: (X, X) => X): (Long, X) = layer.map(x => (1L, x._2)).reduce((a, b) => (a._1 + b._1, sum(a._2, b._2)))

  /**
   * returns a triplet of (#groups, #vertices, ∑X) as an aggregate of the overlay computed by profile or expand
   */
  final def aggregate(overlay: RDD[(HKey, (HKey, Long))]): (Long, Long, Long) = aggregate(overlay, (a: Long, b: Long) => a + b)

  final def aggregate[X](overlay: RDD[(HKey, (HKey, X))], sum: (X, X) => X): (Long, Long, X) = {
    overlay.map { case (vid, (maxVid, x)) => (maxVid, (1L, x)) }
      .reduceByKey((a, b) => (a._1 + b._1, sum(a._2, b._2)))
      .map { case (maxVid, (numCookies, x)) => (1L, numCookies, x) }
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2, sum(a._3, b._3)))
  }

  final def aggregate[X, Y](overlay: RDD[(HKey, (HKey, X))]
                            , combiner: (X, X) => X, converter: (X) => Y, aggregator: (Y, Y) => Y): (Long, Long, Y) = {
    overlay.map { case (vid, (maxVid, x)) => (maxVid, (1L, x)) }
      .reduceByKey((a, b) => (a._1 + b._1, combiner(a._2, b._2)))
      .map { case (maxVid, (numCookies, x)) => (1L, numCookies, converter(x)) }
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2, aggregator(a._3, b._3)))
  }

  /**
   * returns (precision, recall) and prints F1 score
   * TODO f1 needs parallelisation and optimization 1) we're collecting all keys 2) we're turning them to strings
   */
  def f1(modeled: NETWORK, validation: NETWORK): (Double, Double, Double, Double, Double) = {
    val j = modeled.join(validation).cache
    val subsetKeys = j.keys.collect.map(_.asString).toSeq
    val tpfpfn: (Double, Double, Double) = j.map({
      case (vid, (modelEdges, validEdges)) => {
        val subsetModelEdges = modelEdges.map(_._1.asString).intersect(subsetKeys)
        val subsetValidEdges = validEdges.map(_._1.asString).intersect(subsetKeys)
        val tp = subsetModelEdges.intersect(subsetValidEdges).size.toDouble
        val fp = subsetModelEdges.size.toDouble - tp
        val fn = subsetValidEdges.size.toDouble - tp
        (tp, fp, fn)
      }
    }).reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
    j.unpersist(true)
    val precision = tpfpfn._1 / (tpfpfn._1 + tpfpfn._2)
    val recall = tpfpfn._1 / (tpfpfn._1 + tpfpfn._3)
    val F1 = 2 * (precision * recall) / (precision + recall)
    println(s"TP=${tpfpfn._1}, FP=${tpfpfn._2}, FN=${tpfpfn._3}, PRECISION=${precision}, RECALL=${recall}, F1=${F1}")
    (tpfpfn._1, tpfpfn._2, tpfpfn._3, precision, recall)
  }
}