# HiveUDF

使用JDK 17 构建，需下载 [依赖包](https://mvnrepository.com/artifact/org.apache.hive/hive-exec/3.1.3)

1、HiveDecode：提供类似Oracle的decode函数的转码功能 支持多个码值同时转换

2、LinearScoreCalculation：线性得分计算

3、NearestTimeSelect：判断给定的时间在另一组时间内最接近的值

4、TimeRangeMatch：判断给定的时间是否处于一组时间区间内

5、RemoveDupe：提供类似 hive 的 collect_set 函数的数组元素去重功能

使用方法 (以下路径仅为示例, 需要上传 jar 包到实际运行的 hdfs 地址, 然后引用此地址)

```shell
add jar hdfs:///apps/hduser/hive_udf/HiveDecode-1.0.jar;
create temporary function hive_decode as 'com.hive.udf.HiveDecode';

add jar hdfs:///apps/hduser/hive_udf/LinearScoreCalculation-1.0.jar;
create temporary function calculate_score as 'com.hive.udf.LinearScoreCalculation';

add jar hdfs:///apps/hduser/hive_udf/NearestTimeSelect-1.0.jar;
create temporary function time_nearest as 'com.hive.udf.NearestTimeSelect';

add jar hdfs:///apps/hduser/hive_udf/TimeRangeMatch-1.0.jar;
create temporary function time_match as 'com.hive.udf.TimeRangeMatch';

add jar hdfs:///apps/hduser/hive_udf/RemoveDupe-1.0.jar;
create temporary function remove_dupe as 'com.hive.udf.RemoveDupe';
```