package cn.spark.scala.core

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author:rzx
  * Date:2017/6/21
  */
object PersistRDD {
  def main(args: Array[String]): Unit = {
    cacheORPersistRDD()
  }

  def cacheORPersistRDD(): Unit ={
    val conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local")
    val sc = new SparkContext(conf)

    //cache,persist,
    // val docsRDD = sc.textFile("input/docs").cache()
    val docsRDD = sc.textFile("input/docs").persist(StorageLevels.MEMORY_ONLY);
    val startime = System.currentTimeMillis()
    docsRDD.count()
    val endtime = System.currentTimeMillis()
    println("耗时："+(endtime-startime))
    docsRDD.count()
    val endtime2 = System.currentTimeMillis()
    println("耗时："+(endtime2-endtime))
  }
}
