package cn.spark.study.core.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.createStructField;

/**
 * Author:rzx
 * Date:2017/6/29
 */
public class RddToDataFrame {
    public static void main(String[] args) {
    }

    public static void createDataset(){
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("RddToDataFrame");
        //初始化SparkSession
        SparkSession ss = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        //================创建Dataset================================================
        Student stu =new Student("zhansgan",18);
        //DataSet结合了RDD和DataFrame的优点, 并带来的一个新的概念Encoder当序列化数据时,，Encoder产生字节码与off-heap进行交互,
        // 能够达到按需访问数据的效果，而不用反序列化整个对象。
        Encoder<Student> stuEn = Encoders.bean(Student.class);
        //创建一个Dataset,用一个对象List和Encoder
        Dataset<Student> stuDs =  ss.createDataset(Collections.singletonList(stu),stuEn);
        stuDs.show();

        //创建Dataset,用一个数组,并将这个数组的元素+1
        Dataset<Integer> intDs = ss.createDataset(Arrays.asList(1,2,3,4),Encoders.INT());
        Dataset<Integer> transformedDS = intDs.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer call(Integer value) throws Exception {
                return value + 1;
            }
        }, Encoders.INT());
        transformedDS.show();

        //用as函数将一个DataFrame转换成Dataset
        ss.read().json("input/people.json").as(stuEn).show();
        ss.close();
    }

    /**
     * 反射来将RDD转换成Dataset
     */
    private static void reflectRDDToDataset(){
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("reflectRDDToDataset");
        //初始化SparkSession
        SparkSession ss = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        //读取一个文件转换成RDD并映射成一个对象类型的RDD
        JavaRDD<Student> stuRDD = ss.read()
                .textFile("input/people.txt")
                .javaRDD()
                .map(line->{
                    String [] V = line.split(",");
                    return new Student(V[0].trim(),Integer.parseInt(V[1].trim()));
                });
        Dataset<Row> stuDs = ss.createDataFrame(stuRDD,Student.class);
        //把RDD注册为一个临时视图，这样就可以像操作数据库那样操作它
        stuDs.createOrReplaceTempView("studentView");
        //sparksession使用一个sql来查询数据
        //stuDs.select(col("age").gt(20)).show();
        Dataset<Row> stuRow = ss.sql("select * from people where age>20");
        stuRow.show();

        //对Dataset进行映射转换，下标转换
        Dataset<String> stuNameDs = stuRow.map((MapFunction<Row, String>)row->{
            return "name: "+row.getString( 0);
        },Encoders.STRING());
        stuNameDs.show();

        //字段转换
        Dataset<String> stuFileDs = stuRow.map((MapFunction<Row, String>)row->{
           return "name : "+row.<String>getAs("name");
        },Encoders.STRING());

    }

    /**
     * 编程方式来将RDD转换成Dataset
     */
    private static void pagrmRDDToDataset() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("reflectRDDToDataset");
        //初始化SparkSession
        SparkSession ss = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        //创建RDD
        JavaRDD<String> studentRdd = ss.sparkContext().textFile("input/people.txt",1).toJavaRDD();

        //用字符串设置一个schemaschemal
        String schemaString = "name  age";

        //基于字符串的schema构造一个真正的schema结构类型，这个schema类似数据库表结构schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        //将String类型的javaRDD,映射成一行行的数据对应数据库，
        JavaRDD<Row> stuRowRDD =  studentRdd.map(line->{
            String [] v = line.split(",");
            return RowFactory.create(v[0].trim(),v[1].trim());
        });

        //将RDD转化成Dataset,转换之后就可以使用sql来进行查询
        Dataset<Row> stuDs = ss.createDataFrame(stuRowRDD,schema);
        //创建视图
        stuDs.createOrReplaceTempView("student");
        ss.sql("select * from student").show();


    }
}
