package cn.spark.study.core.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Author:rzx
 * Date:2017/6/30
 */
public class DataSourceLoadAndSaveOp {
    public static void main(String[] args) {
        MethodTest();
    }
    //SparkSql支持多种数据源，jdbc,json,parquet...,通过load,save，方法可将这些数据源加载到欸村，保存到文件你
    private static void MethodTest(){
        SparkConf conf =new SparkConf().setMaster("local");
        SparkSession sparkSession = SparkSession.builder()
                .config(conf)
                .appName("DataSourceLoadAndSaveOp")
                .getOrCreate();

        //local加载数据源
        Dataset<Row> userDF = sparkSession.read().load("input/users.parquet");
        //sparkSession.read().parquet("input/users.parquet");
        userDF.show(10);
        /*
        +------+--------------+----------------+
        |  name|favorite_color|favorite_numbers|
        +------+--------------+----------------+
        |Alyssa|          null|  [3, 9, 15, 20]|
        |   Ben|           red|              []|
        +------+--------------+----------------+
        */
        userDF.printSchema();
        /*
        root
         |-- name: string (nullable = true)
         |-- favorite_color: string (nullable = true)
         |-- favorite_numbers: array (nullable = true)
         |    |-- element: integer (containsNull = true)
         */
        //save数据，这里会出现文件存在得情况，提供了SaveModel解决文件已存在的情况
        userDF.select("name","favorite_color").write().format("parquet").save("input/namesAndFavColors.parquet");
       // userDF.select("name","favorite_color").write().parquet("wparquet.prquet");
        //数据源的想互转化,最后的保存结果都是hdfs形式，真不知道format是个什么鬼。
        sparkSession
                .read()
                .json("input/people.json")
                .select("name","age")
                .write()
                .format("parquet").save("input/nameage.parquet");
        sparkSession
                .read()
                .format("json")
                .load("input/people.json")
                .select("name","age")
                .write()
                .format("json").save("input/nameage2.json");

        //对一个文件执行sql;;;;;报错，，什么鬼
        sparkSession.sql("select * FROM  parquet.`input/users.parquet`").show();

        //SaveModelSpark SQL对于save操作，提供了不同的save mode。主要用来处理，当目标位置，已经有数据时，应该如何处理。而且save操作并不会执行锁操作，并且不是原子的，因此是有一定风险出现脏数据的。
        //SaveMode.ErrorIfExists,SaveMode.Append,SaveMode.Overwrite,SaveMode.Ignore
        sparkSession.close();


    }
}

