package cn.spark.scala.core.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author:rzx
  * Date:2017/7/13
  */
object SparkStreamingBSDataSource {
  def main(args: Array[String]): Unit = {
      wordCount()
  }
  def wordCount(){
      val conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
      val ssc  = new StreamingContext(conf,Seconds(1))
      val words = ssc.socketTextStream("localhost",9999)
                          .flatMap((line)=>line.split(" "))
                          .map(x=>(x,1))
      val wordcount = words.reduceByKey(_+_)

      println(131231)
      wordcount.print()
      ssc.start()
      ssc.awaitTermination()
  }

  def wordCountwindow(){
    val conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
    val ssc  = new StreamingContext(conf,Seconds(1))
    val words = ssc.socketTextStream("localhost",9999)
      .flatMap((line)=>line.split(" "))
      .map(x=>(x,1))
    //window操作
    val w3window = words.window(Seconds(3),Seconds(2))
    val wordcount = w3window.reduceByKey(_+_)

    wordcount.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def wordCountRBwindow(){
    val conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
    val ssc  = new StreamingContext(conf,Seconds(1))
    val words = ssc.socketTextStream("localhost",9999)
      .flatMap((line)=>line.split(" "))
      .map(x=>(x,1))

   /* //reduceByKeyAndWindow(reduceFunc,windowDuration,slideDuration)
    val wordcount20 = words.reduceByKeyAndWindow((x1:Int,x2:Int)=>x1+x2,Seconds(20),Seconds(2))
    wordcount20.foreachRDD{
      rd=>rd.foreachPartition(

      )
    }*/
//   wordcount20.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
