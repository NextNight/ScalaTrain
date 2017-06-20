package cn.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author:rzx
  * Date:2017/6/19
  */
object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local")
    val sc = new SparkContext(conf)
    val numList = Array(1,2,3,4,5,6,7,8,9)
    val parRDD = sc.parallelize(numList)
    val sum = parRDD.reduce(_+_)
    println(sum)

  }
}
