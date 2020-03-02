package com.ming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Ch16 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testSQL").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/cache/spark/").getOrCreate();

        // RDD under the hood
//        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
//        dataset.show();
//        dataset.filter("subject = 'Modern Art' AND year >= 2007").show();
//        dataset.filter((FilterFunction<Row>) row -> (row.get(2).equals("Modern Art") && Integer.parseInt(String.valueOf(row.get(3))) > 2007)).show();

//        Column subjectCol = dataset.col("subject");

//        Column subjectCol = functions.col("subject");
//        Column yearCol = dataset.col("year");
//        Dataset<Row> modernArtResults = dataset.filter(subjectCol.equalTo("Modern Art").and(yearCol.geq("2007")));
//        modernArtResults.show();

        List<Row> inMemory = new ArrayList<>();
        inMemory.add(RowFactory.create("WARN", "16 December 2018"));

        StructField[] fields = new StructField[] {
            new StructField("level", DataTypes.StringType, false, Metadata.empty()),
            new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);

        dataset.show();

//        dataset.createOrReplaceTempView("my_students_table");
//
//        Dataset<Row> res = spark.sql("select distinct(score) from my_students_table where subject='French'");
//        res.show();





        spark.close();
    }

}
