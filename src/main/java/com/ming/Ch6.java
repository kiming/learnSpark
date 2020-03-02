package com.ming;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Ch6 {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("Hello: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("Hello: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> originalLogMsgs = sc.parallelize(inputData);
//
////        JavaPairRDD<String, String> pairRdd = originalLogMsgs.mapToPair(rawVal -> {
////            String[] cols = rawVal.split(":");
////            return new Tuple2<>(cols[0], cols[1].trim());
////        });
//
//        JavaPairRDD<String, Long> pairRdd = originalLogMsgs.mapToPair(rawVal -> {
//            String[] cols = rawVal.split(":");
//            return new Tuple2<>(cols[0], 1L);
//        });
//
//        JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey(Long::sum);
//
//        sumsRdd.collect().forEach(tuple -> System.out.println(String.format("%s: %s", tuple._1, tuple._2)));
        sc.parallelize(inputData)
                .mapToPair(rawVal -> new Tuple2<>(rawVal.split(":")[0], 1L))
                .groupByKey()
                .foreach(tuple -> System.out.println(String.format("%s: %s", tuple._1, Iterables.size(tuple._2))));
//                .reduceByKey(Long::sum)
//                .foreach(tuple -> System.out.println(String.format("%s: %s", tuple._1, tuple._2)));

        sc.close();
    }

}
