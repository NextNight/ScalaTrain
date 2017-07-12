package cn.spark.study.core.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Author:rzx
 * Date:2017/7/3
 */
public class SQlFunctions {
    public static void main(String[] args) {
        sqlFunctionsTrain();
    }
    private static void sqlFunctionsTrain(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sqlFunctionsTrain");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //构造sparkSession
        SparkSession sparkSession =SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        //模拟用户访问数据:日期，用户ID
       List<String> list = Arrays.asList(
               "2012-04-01,041110",
               "2012-04-01,041111",
               "2012-04-01,041112",
               "2012-04-01,041113",
               "2012-04-02,041111",
               "2012-04-02,041113",
               "2012-04-02,041112",
               "2012-04-02,041112",
               "2012-04-03,041111"
       );

       //并行化构建RDD
        JavaRDD<String> logRDD = jsc.parallelize(list,2);
        JavaRDD<Row> logrowRDD = logRDD.map(log->{
            return RowFactory.create(log.split(",")[0].trim(),log.split(",")[1].trim());});
        //
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("date",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("ID",DataTypes.StringType,true));
        StructType schema =  DataTypes.createStructType(fields);

        //javaRDD ->DS
        Dataset<Row> logset = sparkSession.createDataFrame(logrowRDD,schema);
        //统计每天的访问量
        logset.groupBy("date").count().show();
        /*
        +----------+-----+
        |      date|count|
        +----------+-----+
        |2012-04-01|    4|
        |2012-04-03|    1|
        |2012-04-02|    4|
        +----------+-----+
        * */
        logset.sort("date").show();
    }
}
