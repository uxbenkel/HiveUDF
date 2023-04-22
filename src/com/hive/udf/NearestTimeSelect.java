package com.hive.udf;
/*
  author       uxbenkel@Github
  createTime   2022-11-02 (1.0) 新建程序
  modifyTime   2023-04-11 (1.1) 逻辑更正 改为先判断G/L
  funtion      判断给定的时间在另一组时间内最接近的值
 */

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;
import java.util.Objects;

public class NearestTimeSelect extends UDF {
    public NearestTimeSelect() {
    }

    // para1 需要判断的时间
    // para2 排序类型(G/L)最接近但大于/最接近但小于
    // para3 需要判断的时间区间 格式 时间1,时间2,时间3...
    public String evaluate(String time, String sortType, String timeRanges) {
        // 设定返回值
        String nearestTime = null;
        // 入参空置处理
        if (time == null || timeRanges == null || ("\\N").equals(time) || ("\\N").equals(timeRanges) || ("").equals(time) || ("").equals(timeRanges)) {
            return null;
        }

        // 处理传入的时间区间 转换为数组并升序处理
        String[] arrayTimeRanges = timeRanges.split(",");
        Arrays.sort(arrayTimeRanges);
        // 最接近但大于的情况
        if (Objects.equals(sortType, "G")) {
            int i = 0;
            while (i <= arrayTimeRanges.length - 1) {
                if (arrayTimeRanges[i].compareTo(time) >= 0) {
                    nearestTime = arrayTimeRanges[i];
                    break;
                }
                i++;
            }
        }
        // 最接近但小于的情况
        if (Objects.equals(sortType, "L")) {
            int i = arrayTimeRanges.length - 1;
            while (i >= 0) {
                if (arrayTimeRanges[i].compareTo(time) <= 0) {
                    nearestTime = arrayTimeRanges[i];
                    break;
                }
                i--;
            }
        }
        return nearestTime;
    }
}