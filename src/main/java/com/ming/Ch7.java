package com.ming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Ch7 {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("Let's call it a party");
        inputData.add("Although we are not ready");
        inputData.add("Otherwise it will become a pity");
        inputData.add("But I'd rather say sorry");
        inputData.add("Because you are so bitchy");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(inputData)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .distinct()
                .collect()
                .forEach(System.out::println);

        sc.close();
    }

}
