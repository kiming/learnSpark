package com.ming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<>();

        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Use all the available cores in the machine
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> originalDoubles = sc.parallelize(inputData);
//        Double val = originalIntegers.reduce(Double::sum);
//        System.out.println(val);
        JavaRDD<Tuple2<Double, Double>> sqrtDoubles = originalDoubles.map(value -> new Tuple2<>(value, Math.sqrt(value)));
        originalDoubles.map(Math::sqrt).collect().forEach(System.out::println);

        sc.close();
    }
}
