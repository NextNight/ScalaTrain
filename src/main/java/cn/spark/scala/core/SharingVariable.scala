package cn.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author:rzx
  * Date:2017/6/21
  */
object SharingVariable {
  def main(args: Array[String]): Unit = {

  }

  def BroadcastVariableTest(): Unit ={
    val conf = new SparkConf().setMaster("local").setMaster("BroadcastVariableTest")
    val sc = new SparkContext(conf)

    val ct = 3
    //把ct编程共享变量
    val ctboard = sc.broadcast(ct)

    val list = Array(1,2,3,4,5,6,7,8)
    val numRDD  = sc.parallelize(list)
    numRDD.map(num=>num*ctboard.value).foreach(n=>println(n))

  }
}
