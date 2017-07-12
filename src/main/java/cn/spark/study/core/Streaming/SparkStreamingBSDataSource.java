package cn.spark.study.core.Streaming;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Author:rzx
 * Date:2017/7/12
 */
public class SparkStreamingBSDataSource {
    public static void main(String[] args) {

    }
    private void ssBSDataSource()throws InterruptedException{
        SparkConf conf = new SparkConf().setAppName("ssBSDataSource").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));

        /*jsc.textFileStream("input/streaming");
        jsc.textFileStream("hdfs://spark/input");*/
        JavaReceiverInputDStream<String> riDstream =   jsc.socketTextStream("localhost",9999);
        JavaDStream<String>    listDstream =  riDstream.flatMap(lines-> Arrays.asList(lines.split(" ")).iterator());
        JavaPairDStream<String,Integer> pairDStream =    listDstream.mapToPair(x->new Tuple2<>(x,1));

        JavaPairDStream<String,Integer> wordCount =  pairDStream.reduceByKey((x1,x2)->(x1+x2));

        wordCount.print();//默认输出前10条数据
        jsc.start();
        jsc.awaitTermination();//不断的不等待一段时间间隔进行一次执行。

        //直到我们使用jsc.close();



    }


}
