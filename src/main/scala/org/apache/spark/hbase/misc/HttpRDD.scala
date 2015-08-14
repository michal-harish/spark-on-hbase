package org.apache.spark.hbase.misc

import java.net.InetSocketAddress

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.rdd.RDD

/**
 * Created by mharis on 14/08/15.
 *
 * This simple tool serves the entire content of RDD as http file so that it can be simply
 * fetchted from R
 */
case class HttpRDD(val rdd: RDD[_], val port: Int = 0) {

  val server = HttpServer.create(new InetSocketAddress(port), 0)
  val address = server.getAddress
  val url = s"http://${address.getHostString}:${address.getPort}/".replace("0:0:0:0:0:0:0:0", "localhost")

  println(s"read.table('${url}', sep =',', header=F)")
  server.createContext("/", RDDHandler)
  server.start()
  RDDHandler.synchronized(RDDHandler.wait)
  server.stop(0)
  println("done.")

  object RDDHandler extends HttpHandler {

    override def handle(t: HttpExchange): Unit = {
      try {
        t.sendResponseHeaders(200, 0)
        val os = t.getResponseBody
        val newLine = "\n".getBytes
        rdd.toLocalIterator.foreach { x => {
          val line: String = x match {
            case (k:Any, p:Product) => k.toString + "," +p.productIterator.mkString(",")
            case (k: Any, i: Iterable[_]) => k.toString + "," + i.mkString(",")
            case i: Iterable[_] => i.mkString(",")
            case p: Product => p.productIterator.mkString(",")
            case _: Any => x.toString
          }
          os.write(line.getBytes)
          os.write(newLine)
        }
        }
        os.close()
      } finally {
        synchronized(notifyAll)
      }
    }

  }

}
