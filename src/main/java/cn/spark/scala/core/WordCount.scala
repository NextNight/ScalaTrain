package cn.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author:rzx
  * Date:2017/6/19
  */
object WordCount {
    def main(args:Array[String]): Unit ={
        val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]");
        val sc = new SparkContext(conf);
        val lines = sc.textFile("input/words");
        val words = lines.flatMap((line) =>line.split(" "));
        val pairs = words.map(word =>(word,1))
        println(pairs)
        val wordCounts = pairs.reduceByKey(_+_);
        wordCounts.foreach(wordcount =>println(wordcount._1+" - "+wordcount._2))
    }
}
