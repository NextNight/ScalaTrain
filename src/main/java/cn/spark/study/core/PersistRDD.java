package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * Author:rzx
 * Date:2017/6/21
 * RDD持久化
 */
public class PersistRDD {
    public static void main(String[] args) {
       // cacheRDD();
        persistRDD();
    }

    /**
     * persist:
     */
    private static void persistRDD(){
        SparkConf conf  = new SparkConf().setAppName("persistRDD").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        //persist()参数设置缓存级别，可以只还存在内存，或者磁盘。。。
        JavaRDD<String> numsRDD =  jsc.textFile("input/docs").persist(StorageLevel.MEMORY_ONLY());

        long startime = System.currentTimeMillis();
        numsRDD.count();
        long endtime = System.currentTimeMillis();
        System.out.printf("耗时："+(endtime-startime));
        numsRDD.count();
        long endtime2 = System.currentTimeMillis();
        System.out.printf("耗时："+(endtime2-endtime));
        jsc.close();
    }
    /**
     * cache:
     */
    private static void cacheRDD(){
        SparkConf conf  = new SparkConf().setAppName("cacheRDD").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        //cache()的位置很重要，必须在构建RDD的时候持久化，不能分开
        /*JavaRDD<String> numsRDD =  jsc.textFile("input/docs")
                .cache();*/
        JavaRDD<String> numsRDD =  jsc.textFile("input/docs");

        long startime = System.currentTimeMillis();
        numsRDD.count();
        long endtime = System.currentTimeMillis();
        System.out.printf("耗时："+(endtime-startime));
        numsRDD.count();
        long endtime2 = System.currentTimeMillis();
        System.out.printf("耗时："+(endtime2-endtime));
        jsc.close();
    }

}
