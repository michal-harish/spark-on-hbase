package org.apache.spark.hbase.demo

import org.apache.spark.SparkContext

object DXPJobRunner extends App {

  /** Execution sequence **/
  try {
    if (args.length == 0) {
      throw new IllegalArgumentException
    }
    val sc = new SparkContext() // hoping to get all configuration passed from scripts/spark-submit
    DEMO.init(sc)
    try {
      val a = args.iterator
      while (a.hasNext) {
        a.next match {
          case arg: String if (!arg.startsWith("-")) => {
            try {
              val methodArgs = arg.split(" ")
              if (methodArgs.length == 1) {
                val m = DEMO.getClass.getMethod(methodArgs(0))
                DEMO.time(m.invoke(DEMO))
              } else {
                val m = DEMO.getClass.getMethod(methodArgs(0), classOf[String])
                DEMO.time(m.invoke(DEMO, methodArgs(1)))
              }
            } catch {
              case e: NoSuchMethodException => println(s"method `${args(0)}` not defined in the DXPJobRunner")
            } finally {
              sc.stop
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


}