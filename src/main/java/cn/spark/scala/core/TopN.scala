package cn.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author:rzx
  * Date:2017/6/21
  */
object TopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setMaster("TopN")
    val sc = new SparkContext(conf)

    val linesRDD = sc.textFile("input/numsort")
    //map-> sort->take ->print
    linesRDD.map(line=>(line.toInt,line)).sortByKey(false).take(5).foreach(x=>println(x._1))
  }
}
