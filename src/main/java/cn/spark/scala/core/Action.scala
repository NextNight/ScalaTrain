package cn.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author:rzx
  * Date:2017/6/20
  */
object Action {
  def main(args: Array[String]): Unit = {
    //collectTest()
    //takeTest();
    countByKeyTest
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
    //count
    val count = numRDD.count()
    println("元素个数："+count);
    val doubleRDD = numRDD.map(x=>x*2)
    //collect
    val listNum =  doubleRDD.collect()
    listNum.foreach(
      num=>println(num)
    )
  }

  /**
    * take(N)算子：将RDD的前N条记录拉取到本地
    */
  def takeTest(): Unit = {
    val conf = new SparkConf().setAppName("takeTest").setMaster("local")
    val sc = new SparkContext(conf)
    val numList = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numRDD = sc.parallelize(numList)
    val num5 = numRDD.take(5)
    for (n <- num5) {
      println(n)
    }
   // num5.foreach(n => print(n + " "))
  }
  /**
    * saveAsTextFile算子：RDD保存到文件
    *
    */
  def saveAsTextFileTest(): Unit = {
    val conf = new SparkConf().setAppName("takeTest").setMaster("local")
    val sc = new SparkContext(conf)
    val numList = Array("TaskSchedulerImpl","DAGScheduler")
    val numRDD = sc.parallelize(numList)
    numRDD.saveAsTextFile("input/scalasaveAsTextFile")
  }

  /**
    * countByKey算子：统计
    *
    */
  def countByKeyTest(): Unit = {
    val conf = new SparkConf().setAppName("takeTest").setMaster("local")
    val sc = new SparkContext(conf)
    val numList = Array("TaskSchedulerImpl","DAGScheduler","TaskSchedulerImpl")
    val numRDD = sc.parallelize(numList)

    numRDD.countByValue().foreach(num=>println(num._1+"  "+num._2));
  }
}
