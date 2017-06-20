package cn.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author:rzx
  * Date:2017/6/20
  */
object Transformation {
  def main(args: Array[String]): Unit = {
   // mapTest()
   // filterTest()
    // flatMapTest()
    //groupByKeyTest()
    joinAndCoGroupTest()
  }

  /**
    * map：对集合进行一种函数映射
    * map算子：让集合中的所有元素*2
    */
  def mapTest(){
    val conf = new SparkConf().setAppName("mapTest").setMaster("local")
    val sc   = new SparkContext(conf)
    val list = Array(1,2,3,4,5,6,7,8)
    val numRDD = sc.parallelize(list)
    val num2RDD = numRDD.map(num=>num*2)
    num2RDD.foreach(num2=>print(num2+" "))
  }

  /**
    * filter算子：过滤/选择符合条件的数据
    */
  def filterTest(): Unit ={
    val conf = new SparkConf().setAppName("filterTest").setMaster("local")
    val sc   = new SparkContext(conf)
    val list = Array(1,2,3,4,5,6,7,8,9,10)
    val numRDD = sc.parallelize(list)
    //filter过滤偶数
    val evenNumRDD = numRDD.filter(num=> num % 2 == 0)
    evenNumRDD.foreach(evennNum => print(evennNum + " "))
  }

  /**
    * flatMap算子：
    */
  def flatMapTest(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("flatMapTest")
    val sc = new SparkContext(conf)
    val list = Array("hello world", "hello you", "good morning")
    val wordsRDD = sc.parallelize(list)
    val wordRDD = wordsRDD.flatMap(words=>words.split(" "))
    wordRDD.foreach(word=>println(word).toString)
  }

  /**
    * groupBykey算子：按tuple的key进行分组
    */
  def groupByKeyTest(): Unit ={
    val conf = new SparkConf().setAppName("groupByKeyTest").setMaster("local")
    val sc = new SparkContext(conf)
    val list = Array(
      Tuple2("A",89),
      Tuple2("c",59),
      Tuple2("B",74),
      Tuple2("c",50),
      Tuple2("A",98))

    val scoreRDD = sc.parallelize(list)
    //不加sortByKey结果乱序
    // val groupRDD = scoreRDD.groupByKey()
    val groupRDD = scoreRDD.groupByKey().sortByKey()
    groupRDD.foreach(
      scoreTP=> {
        print(scoreTP._1 + ": ")
        scoreTP._2.foreach(score=>print(score+" "))
        println()
      })
  }
  /**
    * reduceByKey算子：按tuple的key进行分组
    */
  def reduceByKeyTest(): Unit ={
    val conf = new SparkConf().setAppName("reduceByKeyTest").setMaster("local")
    val sc = new SparkContext(conf)
    val list = Array(
      Tuple2("A",89),
      Tuple2("c",59),
      Tuple2("B",74),
      Tuple2("c",50),
      Tuple2("A",98))

    val scoreRDD = sc.parallelize(list)
    val totalRDD = scoreRDD.reduceByKey(_+_).sortByKey()
    totalRDD.foreach(
      total=>println(total._1+": "+total._2)
    )
  }
  /**
    * join算子：按tuple的key进行分组
    */
  def joinAndCoGroupTest(): Unit ={
    val conf = new SparkConf().setAppName("joinAndCoGroupTest").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(
      Tuple2(1,89),
      Tuple2(2,59),
      Tuple2(3,74),
      Tuple2(4,50),
      Tuple2(5,98)
    )
    val stuList = Array(
      Tuple2(1,"A"),
      Tuple2(2,"B"),
      Tuple2(3,"C"),
      Tuple2(4,"D"),
      Tuple2(6,"E")
    )

    val scoreRDD = sc.parallelize(scoreList)
    val stuRDD = sc.parallelize(stuList)
    //join联合两个RDD
    val joinRDD = scoreRDD.join(stuRDD).sortByKey()
    joinRDD.foreach(
      join=>println(join.toString())
    )
    //cogroup联合两个RDD
    val cogroupRDD = scoreRDD.cogroup(stuRDD).sortByKey()
    cogroupRDD.foreach(
      co=>println(co.toString())
    )
  }
}
