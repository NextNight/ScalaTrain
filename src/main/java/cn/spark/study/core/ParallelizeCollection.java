package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Author:rzx
 * Date:2017/6/19
 * 并行化计算
 */
public class ParallelizeCollection {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("ParallelizeCollection")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> parallelizeRDD = sc.parallelize(Arrays.asList(1,2,3,4,5));

        //累加，执行reduce算子操作
        int sum = parallelizeRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        int sum8 =parallelizeRDD.reduce((x1,x2)->x1+x2);

        System.out.println("1+...5 = "+sum8);
    }
}
