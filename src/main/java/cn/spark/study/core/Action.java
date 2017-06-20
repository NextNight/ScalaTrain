package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Author:rzx
 * Date:2017/6/20
 */
public class Action {
    public static void main(String[] args) {
        reduceTest();
        collectTest();
        takeTest();
        savaAsTextFileTest();
    }

    /**
     * reduce算子：1，2聚合结果和3聚合结果和4聚合，递归
     */
    private static void reduceTest(){
        SparkConf conf = new SparkConf().setAppName("reduceTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> numList = Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> numRDD = jsc.parallelize(numList);

        Integer sumRDD = numRDD.reduce((x1, x2)->x1+x2);
        System.out.printf(sumRDD.toString());

        jsc.close();
    }
    /**
     * count:统计RDD元素数量
     * collect算子：将RD数据拉取到本地
     * 大量数据时，性能比较差，高Io，或者oom内存溢出
     */
    private static void collectTest(){
        SparkConf conf = new SparkConf().setAppName("collectTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> numList = Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> numRDD = jsc.parallelize(numList);

        //count:
        long count = numRDD.count();
        System.out.printf("元素个数："+count);

        //map:元素翻倍
        JavaRDD<Integer> doubleNumRDD = numRDD.map(x->x*2);

        //collect:将RDD的数据拉取到本地，变成了java的List,
        List<Integer> listNum= doubleNumRDD.collect();
        listNum.forEach(
                num-> System.out.printf(num.toString())
        );

        jsc.close();
    }

    /**
     *
     * take(N),类似collect,把RDD拉取到本地，取前N条数据
     */
    private  static void takeTest(){
        SparkConf conf = new SparkConf().setAppName("takeTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> numList = Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> numRDD = jsc.parallelize(numList);

        //take(N):
        List<Integer> list = numRDD.take(8);
        list.forEach(
                num-> System.out.printf(num+" ")
        );
        jsc.close();
    }

    /**
     * savaAsTextFile:保存到文件
     */
    private static void savaAsTextFileTest(){
        SparkConf conf = new SparkConf().setAppName("countByKeyTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String > list = Arrays.asList("hello world","how are you","nice to");
        JavaRDD<String> strRDD =  jsc.parallelize(list);
        //saveAsTextFile
        strRDD.saveAsTextFile("input/str.txt");

        jsc.close();
    }

    /**
     * countByvalue:统计每个key的数量
     */
    private static void countByKeyTest(){
        SparkConf conf = new SparkConf().setAppName("countByKeyTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<Integer,Integer>> numList = Arrays.asList(
                new Tuple2<>(1,100),
                new Tuple2<>(2,50),
                new Tuple2<>(3,100),
                new Tuple2<>(4,80),
                new Tuple2<>(5,50),
                new Tuple2<>(6,80)
        );
        JavaRDD<Tuple2<Integer,Integer>> numRDD  = jsc.parallelize(numList);
        numRDD.countByValue();
    }

}
