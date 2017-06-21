package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Author:rzx
 * Date:2017/6/21
 */
public class TopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TopN").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //sortByKey()默认由小到大，参数false，有大到小
        jsc.textFile("input/numsort")
                .mapToPair(line->new Tuple2<>(Integer.valueOf(line),line))
                .sortByKey(false)
                .take(5)
                .forEach(n-> System.out.println(n._1.toString()));

        jsc.close();
    }
}
