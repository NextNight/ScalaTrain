package cn.spark.study.core.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;

import java.util.HashMap;
import java.util.Map;

/**
 * Author:rzx
 * Date:2017/7/3
 */
public class HiveDataSource {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("HiveDataSource")
                .master("local")
                .config("spark.sql.warehouse.dir", "spark-warehouse")
                .enableHiveSupport()
                .getOrCreate();
        sparkSession.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING)");
        sparkSession.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src");

        Dataset<Row> srcDS = sparkSession.sql("select * from src");
        Dataset<String> srcstrDS= srcDS.map((MapFunction<Row,String>) row->{
            return "key:"+row.get(0)+"vlaue:"+row.get(1);
        },Encoders.STRING());

        srcstrDS.show();

        sparkSession.close();
    }


    private static void JDBCDataSource(){
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("JDBCDataSource")
                .master("local")
                .getOrCreate();
        Map options = new HashMap<String,String>();
        options.put("url","jdbc:mysql://spark1:3306/textdb");
        options.put("dbtable","student");

    }


}
