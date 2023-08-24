package com.hive.udf;
/*
  author       uxbenkel@Github
  createTime   2022-08-09 (1.0) 新建程序
  modifyTime
  funtion      线性得分计算
 */

import org.apache.hadoop.hive.ql.exec.UDF;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Objects;
import java.util.Collections;

public class LinearScoreCalculation extends UDF {
    public LinearScoreCalculation() {
    }

    // para1 需要计算的指标值
    // para2 得分随指标值升序/降序(A/D)
    // para3 给定的一组指标值 即x值 用逗号隔开
    // para4 给定的一组得分值 即y值 用逗号隔开
    public String evaluate(String indValue, String sortType, String stringRanges, String stringScores) {
        // 设定返回值
        String indScore;

        // 入参空值处理
        if (indValue == null || sortType == null || stringRanges == null || stringScores == null || ("\\N").equals(stringRanges) || ("\\N").equals(stringScores) || ("\\N").equals(indValue) || ("\\N").equals(sortType) || stringRanges.isEmpty() || stringScores.isEmpty()) {
            return null;
        }

        // 处理传入的指标值 转换为数组并升序处理
        String[] arrayRanges = stringRanges.split(",");
        BigDecimal[] ranges = new BigDecimal[arrayRanges.length];
        for (int i = 0; i < arrayRanges.length; i++) {
            ranges[i] = new BigDecimal(arrayRanges[i]);
        }
        Arrays.sort(ranges);

        // 处理传入的得分值 转换为数组并根据第2个参数升序或者降序处理
        String[] arrayScores = stringScores.split(",");
        BigDecimal[] scores = new BigDecimal[arrayScores.length];
        for (int i = 0; i < arrayScores.length; i++) {
            scores[i] = new BigDecimal(arrayScores[i]);
        }
        if (Objects.equals(sortType, "A")) {
            Arrays.sort(scores);
        } else if (Objects.equals(sortType, "D")) {
            Arrays.sort(scores, Collections.reverseOrder());
        } else {
            return null;
        }

        // 如果指标值小于指标区间最小值或者大于指标区间最大值 直接返回得分区间对应的最小值最大值
        BigDecimal value = new BigDecimal(indValue);
        if (value.compareTo(ranges[0]) <= 0) {
            indScore = scores[0].toString();
            return indScore;
        } else if (value.compareTo(ranges[ranges.length - 1]) >= 0) {
            indScore = scores[ranges.length - 1].toString();
            return indScore;
        }

        // 判断指标值在给定的区间位置;第 index 组
        int index = 0;
        for (int i = 1; i < ranges.length; i++) {
            if (value.compareTo(ranges[i]) <= 0) {
                index = i - 1;
                break;
            }
        }

        // 确定指标值所在区间后 通过对应的 x,y 值求出 k,b 值 计算得分
        BigDecimal k;
        BigDecimal b;

        BigDecimal x1 = ranges[index];
        BigDecimal x2 = ranges[index + 1];
        BigDecimal y1 = scores[index];
        BigDecimal y2 = scores[index + 1];

        k = (y1.subtract(y2)).divide(x1.subtract(x2), 8, RoundingMode.HALF_UP);
        b = ((x1.multiply(y2)).subtract(x2.multiply(y1))).divide(x1.subtract(x2), 8, RoundingMode.HALF_UP);
        indScore = k.multiply(value).add(b).toString();

        // System.out.println("k=" + String.format("%.2f", k) + " b=" + String.format("%2f", b) + " y=" + String.format("%.2f", new BigDecimal(indScore)));

        return indScore;
    }
}
