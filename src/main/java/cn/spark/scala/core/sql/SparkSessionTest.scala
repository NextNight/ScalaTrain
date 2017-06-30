package cn.spark.scala.core.sql

import org.apache.spark.sql.SparkSession
/**
  * Author:rzx
  * Date:2017/6/29
  */

object SparkSessionTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .config("12312","123213")
      .getOrCreate()

    //导入隐式转换。。包。。。
    import sparkSession.implicits._

    val df =sparkSession.read.json("input/people.json")
    df.show()
    df.select("name","age").show(2)
    df.select(df("name"),df("age")+1).show(2)
    df.select($"name", $"age" + 1).show()
    df.filter($"age">20).show()
    //读取csv
    val dfCvs = sparkSession.read.csv("input/Advertising.csv")
    dfCvs.show()
    //读取csv设置头部
    val dfOptions = sparkSession.read.option("header","true").csv("input/Advertising.csv")
    dfOptions.show()

  }
}
