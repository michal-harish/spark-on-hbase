/**
 * this script is enetered with sc already defined
 */

import org.apache.spark.hbase.examples.graph.DemoGraphApp

import scala.math._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.HConstants._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase._
import org.apache.spark.hbase.helpers._
import org.apache.spark.hbase.misc.RDDUtils._
import org.apache.spark.hbase.misc.ByteUtils._
import org.apache.spark.hbase.examples.simple._
import org.apache.spark.storage.StorageLevel._


/**
 * Initialise DEMO within the current spark shell context and import all public members into the shell's global scope
 */
val app = new DemoSimpleApp(sc)

import app._

import HBaseTableSimple._

help