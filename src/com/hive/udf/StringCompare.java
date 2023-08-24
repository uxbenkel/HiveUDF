package com.hive.udf;
/*
  author       uxbenkel@Github
  createTime   2023-07-08 (1.0) 新建程序
  modifyTime
  funtion      提供两个字符串之间的比较，输出相似度
 */

import org.apache.hadoop.hive.ql.exec.UDF;


public class StringCompare extends UDF {
    public StringCompare() {
    }

    // para1 需要处理的字符串A
    // para2 需要处理的字符串B
    // para3 比率（相似度）类型 MAX为（相同字符数量/较大字符串长度）MIN为（相同字符数量/较小字符串长度）
    public Double evaluate(String stringA, String stringB, String rateType) {

        // 设定返回值
        Double rate = null;

        // 入参空值处理
        if (stringA == null || ("\\N").equals(stringA) || stringA.isEmpty() || stringB == null || ("\\N").equals(stringB) || stringB.isEmpty()) {
            return null;
        }

        // 设定相同字符计数
        int matchCharCount = 0;

        // 循环比较字符是否相同
        for (String charA : stringA.split("")) {
            for (String charB : stringB.split("")) {
                if (charA.equals(charB)) {
                    matchCharCount++;
                    break;
                }
            }
        }

        // 处理比率（相似度）计算结果
        if ("MAX".equals(rateType)) {
            rate = matchCharCount / (double) Math.max(stringA.length(), stringB.length());
        } else if ("MIN".equals(rateType)) {
            rate = matchCharCount / (double) Math.min(stringA.length(), stringB.length());
        }

        // 返回结果
        return rate;
    }
}