package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Author:rzx
 * Date:2017/6/20
 * Transformation算子操作实战
 */
public class Transformation {
    public static void main(String[] args) {
        mapTest();
        filterTest();
        flatMapTest();
        groupByKeyTest();
        reduceByKeyTest();
        joinAndCoGroupTest();
    }

    /**
     * map算子：给集合每个元素*2
     */
    private static void mapTest() {
        SparkConf conf = new SparkConf().setAppName("mapTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        JavaRDD<Integer> numRDD = jsc.parallelize(list);
        JavaRDD<Integer> num2RDD = numRDD.map(num -> num * 2);

        num2RDD.foreach(num2 -> System.out.printf(num2.toString() + " "));
        jsc.close();
    }

    /**
     * fileter算子：满足过滤条件的保留
     */
    private static void filterTest() {
        SparkConf conf = new SparkConf().setAppName("filterTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numRDD = jsc.parallelize(list);
        //这里用返回值是因为要判断过滤的结果是保留丢弃，true则保留
        JavaRDD<Integer> num2RDD = numRDD.filter(num -> num % 2 == 0);
        num2RDD.foreach(num2 -> System.out.println(num2 + " "));

        jsc.close();
    }

    /**
     * flatmap算子：接收RDD中所有元素，并进行运算，然后返回多个元素
     * 这里是对元素分割成单词
     */
    private static void flatMapTest() {
        SparkConf conf = new SparkConf()
                .setAppName("flatMapTest")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("hello world", "hello you", "good morning");
        JavaRDD<String> wordsRDD = jsc.parallelize(list);

        JavaRDD<String> wordRDD = wordsRDD.flatMap(
                words -> Arrays.asList(words.split(" ")).listIterator()
        );

        wordRDD.foreach(
                word -> System.out.printf(word+" ")
        );

        jsc.close();
    }

    /**
     * groupByKey算子：按key分组,
     * sortByKey:按key排序
     */
    private static void groupByKeyTest() {
        SparkConf conf = new SparkConf().setAppName("groupByKeyTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> list = Arrays.asList(
                new Tuple2<>("A",88),
                new Tuple2<>("B",78),
                new Tuple2<>("C",55),
                new Tuple2<>("A",95),
                new Tuple2<>("C",34)
        );
        JavaPairRDD<String,Integer> scoreRDD = jsc.parallelizePairs(list);

        //不加sortBykey()结果乱序
        //JavaPairRDD<String,Iterable<Integer>> scoreGroupRDD = scoreRDD.groupByKey();
        JavaPairRDD<String,Iterable<Integer>> scoreGroupRDD = scoreRDD.groupByKey().sortByKey();

        scoreGroupRDD.foreach(
                scoreGroup-> {
                    System.out.printf(scoreGroup._1 + ": ");
                    scoreGroup._2.forEach(score-> System.out.printf(score+" "));
                    System.out.println();
                }
        );

        jsc.close();

    }

    /**
     * reduceByKey算子：按key分组并聚合，即key相同则V相加,
     */
    private static void reduceByKeyTest() {
        SparkConf conf = new SparkConf().setAppName("reduceByKeyTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> list = Arrays.asList(
                new Tuple2<>("A",88),
                new Tuple2<>("B",78),
                new Tuple2<>("C",55),
                new Tuple2<>("A",95),
                new Tuple2<>("C",34)
        );
        JavaPairRDD<String,Integer> scoreRDD = jsc.parallelizePairs(list);

        JavaPairRDD<String,Integer> scoreGroupRDD = scoreRDD.reduceByKey((v1,v2)->v1+v2);

        scoreGroupRDD.foreach(
                score->System.out.println(score._1+": "+score._2)
        );

        jsc.close();

    }

    /**
     * join算子：关联兩個RDD
     * cogroup算子;
     */
    private static void joinAndCoGroupTest(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("joinAndCoGroupTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1,88),
                new Tuple2<>(2,98),
                new Tuple2<>(3,75),
                new Tuple2<>(4,84),
                new Tuple2<>(6,77)
        );
        List<Tuple2<Integer,String>> stuList = Arrays.asList(
                new Tuple2<>(1,"A"),
                new Tuple2<>(2,"B"),
                new Tuple2<>(3,"C"),
                new Tuple2<>(4,"D"),
                new Tuple2<>(5,"E")
        );
        JavaPairRDD<Integer,Integer> scoreRDD = jsc.parallelizePairs(scoreList);
        JavaPairRDD<Integer,String> stuRDD = jsc.parallelizePairs(stuList);

        //join链接操作(通过Key做内链接，保留都存在的key)
        JavaPairRDD  scoreStuRDD= scoreRDD.join(stuRDD).sortByKey();
        scoreStuRDD.foreach(
                score-> System.out.println(score.toString())
        );

        JavaPairRDD  stuScoreRDD= stuRDD.join(scoreRDD).sortByKey();
        scoreStuRDD.foreach(
                score-> System.out.println(score.toString())
        );

        //cogroup连接操作:保留所有的key
        JavaPairRDD  scoreStuCGRDD= scoreRDD.cogroup(stuRDD).sortByKey();
        scoreStuCGRDD.foreach(
                score-> System.out.println(score.toString())
        );
        JavaPairRDD  stuScoreCGRDD= stuRDD.cogroup(scoreRDD).sortByKey();
        scoreStuCGRDD.foreach(
                score-> System.out.println(score.toString())
        );
    }
}
