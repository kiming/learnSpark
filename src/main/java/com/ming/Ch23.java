package com.ming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class Ch23 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/cache/spark/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

//        dataset = dataset.select("level", "datetime");
//        dataset = dataset.selectExpr("level", "date_format(datetime, 'MMMM')");
        dataset = dataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
        dataset = dataset.groupBy(col("level"), col("month"), col("monthnum")).count();
        dataset = dataset.orderBy("monthnum", "level");
        dataset = dataset.drop("monthnum");
        dataset.show(100);

        spark.close();
    }

}
