/**
 * this script is enetered with sc already defined
 */
import scala.math._
import org.apache.spark.hbase.DEMO
import org.apache.spark.hbase.common._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.storage.StorageLevel._


/**
 * Initialise DEMO within the current spark shell context and import all public members into the shell's global scope
 */
DEMO.init(sc)
import DEMO._
