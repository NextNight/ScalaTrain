package cn.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author:rzx
  * Date:2017/6/20
  */
object Action {
  def main(args: Array[String]): Unit = {
    collectTest()
  }

  /**
    * reduce算子:聚和操作
    */
  def reduceTest(): Unit ={
    val conf = new SparkConf().setAppName("reduceTest").setMaster("local")
    val sc = new SparkContext(conf)
    val numList = Array(1,2,3,4,5,6,7,8,9)
    val parRDD = sc.parallelize(numList)
    val sum = parRDD.reduce(_+_)
    println(sum)
  }
  /**
    * count:统计RDD元素个数
    * collect算子:吧RDD数据拉取到本地，大量数据时，性能比较差，高Io，或者oom内存溢出
    */
  def collectTest(): Unit ={
    val conf = new SparkConf().setAppName("collectTest").setMaster("local")
    val sc = new SparkContext(conf)
    val numList = Array(1,2,3,4,5,6,7,8,9)
    val numRDD = sc.parallelize(numList)
    val count = numRDD.count()
    println("元素个数："+count);
    val doubleRDD = numRDD.map(x=>x*2)
    val listNum =  doubleRDD.collect()
    listNum.foreach(
      num=>println(num)
    )
  }


}
