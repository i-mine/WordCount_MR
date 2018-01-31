#!/usr/bin/env bash
hadoop fs -rm -r s3://mob-emr-test/lei.du/result/
hadoop jar  ../libs/WordCount_MR-1.0-SNAPSHOT.jar com.mobvista.dataplatform.Main \
        -D mapreduce.map.memory.mb=1024 \
        -D mapreduce.reduce.memory.mb=1024 \
        -D mapreduce.reduce.java.opts='-XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -Xmx1024m -Xms512m' \
        -D mapreduce.map.java.opts='-XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -Xmx1024m -Xms512m' \
        s3://mob-emr-test/lei.du/data/words.txt \
        s3://mob-emr-test/lei.du/result
