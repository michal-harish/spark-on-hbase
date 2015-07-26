package org.apache.spark.hbase.demo

import org.apache.spark.SparkContext

object DemoJobRunner extends App {

  /** Execution sequence **/
  try {
    if (args.length == 0) {
      throw new IllegalArgumentException
    }
    implicit val context = new SparkContext() // hoping to get all configuration passed from scripts/spark-submit
    val DEMO = new Demo
    try {
      val a = args.iterator
      while (a.hasNext) {
        a.next match {
          case arg: String if (!arg.startsWith("-")) => {
            try {
              val methodArgs = arg.split(" ")
              if (methodArgs.length == 1) {
                val m = DEMO.getClass.getMethod(methodArgs(0))
                time(m.invoke(DEMO))
              } else {
                val m = DEMO.getClass.getMethod(methodArgs(0), classOf[String])
                time(m.invoke(DEMO, methodArgs(1)))
              }
            } catch {
              case e: NoSuchMethodException => println(s"method `${args(0)}` not defined in the DXPJobRunner")
            } finally {
              context.stop
            }
          }
        }
      }
    } finally {
      DEMO.context.stop
    }
  } catch {
    case e: IllegalArgumentException => {
      println("Usage:")
      println("./spark-submit [-p] [-l|-xl|-xxl]  \"<command [argument]>\" ")
    }
  }

  def time[A](a: => A): A = {
    val l = System.currentTimeMillis
    val r = a
    println((System.currentTimeMillis - l).toDouble / 1000)
    r
  }

}