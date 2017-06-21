package cn.spark.study.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Author:rzx
 * Date:2017/6/21
 * 共享变量：
 */
public class SharingVariable {
    public static void main(String[] args) {

    }

    /**
     * 广播变量：broadcast Variable
     *
     */
    private static void broadCastVariableTest(){
        SparkConf conf = new SparkConf().setAppName("broadCastVariableTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        Integer  ct =3;
        //---------------------设置ct共享变量，只读。。通过value()函数获取值
        final Broadcast<Integer> ctboard =  jsc.broadcast(ct);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> numRDD = jsc.parallelize(list);

        //给集合中的每个数字成以ct,这样的话每一个task都要copy一个ct,使用共享变量的话会给每一个partition拷贝一个副本
        numRDD.map(n->n*ctboard.value()).foreach(rs-> System.out.println(rs));

        jsc.close();
    }
    /**
     * 累加变量：Accumulator
     */
    private static void Accumulator(){
        SparkConf conf = new SparkConf().setAppName("broadCastVariableTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        Accumulator<Integer> sum  = jsc.accumulator(0);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> numRDD = jsc.parallelize(list);

        //调用add方法累加
        numRDD.foreach(n->sum.add(n));
        //调用value方法获取值
        System.out.printf(sum.value().toString());

        jsc.close();
    }
}
