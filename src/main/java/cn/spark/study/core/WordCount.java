package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Author:rzx
 * Date:2017/6/19
 */
public class WordCount {
    public static void main(String [] args){
        SparkConf conf = new SparkConf()
                .setAppName("WountCountLocal")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //输入源，创建初始的RDD.分散到partiting.
    /*    JavaRDD<String> lines = sc.textFile("input/words");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).listIterator();
            }
        });

        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        //JavaPairRDD<String,Integer> pairs = words.mapToPair(word->{return new Tuple2<String, Integer>(word, 1)};
        //JavaPairRDD<String,Integer> pairs = words.mapToPair(word->new Tuple2<String, Integer>(word, 1);

        final JavaPairRDD wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        //action
        wordsCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            public void call(Tuple2<String,Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+" - "+wordCount._2);
            }
        });
        sc.close();
*/

        JavaRDD<String> lines = sc.textFile("input/words");
        JavaRDD<String> words =
                lines.flatMap(line -> Arrays.asList(line.split(" ")).listIterator());
        JavaPairRDD<String, Integer> counts =
                words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                        .reduceByKey((x, y) -> x + y);
        counts.foreach(cn -> System.out.println(cn._1+" "+cn._2));
       /* //进行K-V反转,
        JavaPairRDD<Integer,String> resverCounts = counts.mapToPair(count->{return new Tuple2<Integer,String>(count._2,count._1);});
        //按K排序
        resverCounts.sortByKey(false).foreach(scount-> System.out.println(scount._1+" "+scount._2));*/
    }
}
