package cn.spark.study.core.Streaming;

import org.apache.spark.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import scala.Tuple2;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

/**
 * Author:rzx
 * Date:2017/7/12
 */
public class SparkStreamingBSDataSource {
    public static void main(String[] args) throws InterruptedException{
        wordCount();
    }
    private static  void wordCount() throws InterruptedException{
        SparkConf conf = new SparkConf().setAppName("ssBSDataSource").setMaster("local[*]");
        //创建JavaStreamingContext，设置时间延迟为1秒(每次收集前一秒的数据)
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));

        /*jsc.textFileStream("input/streaming");//file
          jsc.textFileStream("hdfs://...");     //hdfs
        */
        //从Socket中获取数据，监听端口9999，的得到的是ReceiverInputDStream
        JavaReceiverInputDStream<String> lines =   jsc.socketTextStream("localhost",9999);
        //接下来就是wordcount的操作了。
        JavaDStream<String> listDstream =  lines.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String,Integer> pairDStream =  listDstream.mapToPair(x->new Tuple2<>(x,1));
        //window
        JavaPairDStream<String,Integer> pairDStreamWindows = pairDStream.window(Durations.seconds(3),Durations.seconds(2));

        JavaPairDStream<String,Integer> wordCount =  pairDStreamWindows.reduceByKey((x1,x2)->(x1+x2));

        wordCount.print();//默认输出前10条数据
        jsc.start();
        jsc.awaitTermination();//不断的不等待一段时间间隔进行一次执行。

        //直到我们使用jsc.close();
    }

    private static  void wordCountUpdateStateByKey() throws InterruptedException{
        SparkConf conf = new SparkConf().setAppName("ssBSDataSource").setMaster("local[*]");
        //创建JavaStreamingContext，设置时间延迟为1秒(每次收集前一秒的数据)
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
       //checkpoint
        jsc.checkpoint("hdfs://spark:9000/checkPointDir");

        //从Socket中获取数据，监听端口9999，的得到的是ReceiverInputDStream
        JavaReceiverInputDStream<String> lines =   jsc.socketTextStream("localhost",9999);
        //接下来就是wordcount的操作了。
        JavaDStream<String> listDstream =  lines.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String,Integer> pairDStream =  listDstream.mapToPair(x->new Tuple2<>(x,1));
        //到了这一步不在调用
        JavaPairDStream<String,Integer> wordCount =  pairDStream.updateStateByKey(
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    public Optional<Integer> call(List<Integer> values,Optional<Integer> state) throws Exception {
                        int vl =0;
                        if(state.isPresent()){
                            vl = state.get();
                        }
                        for(Integer value : values) {
                            vl += value;
                        }

                        return Optional.of(vl);
                    }
                });
        JavaPairDStream<String,Integer> wordCount2 =  pairDStream.updateStateByKey(
                (values,state)->{
                    int vl =0;
                    if(state.isPresent()){
                        vl = state.get();
                    }
                    for(Integer value : values) {
                        vl += value;
                    }

                    return Optional.of(vl);
                 });
        wordCount.print();//默认输出前10条数据
        jsc.start();
        jsc.awaitTermination();//不断的不等待一段时间间隔进行一次执行。

        //直到我们使用jsc.close();
    }
}

