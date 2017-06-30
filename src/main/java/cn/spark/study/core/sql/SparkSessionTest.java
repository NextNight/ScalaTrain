package cn.spark.study.core.sql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
/**
 * Author:rzx
 * Date:2017/6/29
 */
public class SparkSessionTest {
    public static void main(String[] args) throws AnalysisException {
       SparkSession ss = SparkSession
               .builder()
               .appName("ss")
               .master("local")
               .config("asd","133")
               .getOrCreate();
       Dataset<Row> df = ss.read().json("input/people.json");
       df.printSchema();
       df.show();
       df.select(col("name"), col("age").plus(1)).show();
       df.filter(col("age").gt(20)).show();
       df.groupBy("age").count().show();

       // 注册一个DataFrame用sql临时视图
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlRs =  ss.sql("select * from people");
        sqlRs.show();

        //创建一个全局的临时视图
        df.createGlobalTempView("people");
        //临时施图用于global_tem作为一个数据库保存
        Dataset<Row> sqlRsdb =ss.sql("SELECT * FROM global_temp.people");
        sqlRsdb.show();

    }
}
