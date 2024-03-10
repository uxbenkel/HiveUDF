package com.hive.udf;
/*
  author       uxbenkel@Github
  createTime   2024-03-10 (1.0) 新建程序
  modifyTime
  funtion      增强的日期时间求差
 */

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

public class EnhancedDateDiff extends UDF {
    public EnhancedDateDiff() {
    }

    /**
     * @param dateTimeString1 日期时间1
     * @param dateTimeString2 日期时间2
     * @param resultFormats   缺省值,结果单位 默认为 s, 可选 s-秒,m-分,h-小时,d-天,w-周,M-月,y-年
     * @return 返回结果
     */
    public Double evaluate(String dateTimeString1, String dateTimeString2, String... resultFormats) {

        // 设定返回值和缺省值
        Double result;
        long longResult;
        String resultFormat = "s";
        List<String> resultFormatList = Arrays.asList("s", "m", "h", "d", "w", "M", "y");
        String dateTimeFormat;
        List<Integer> dateTimeLength = Arrays.asList(8, 10, 19);


        // 入参空值处理
        if (dateTimeString1 == null || ("\\N").equals(dateTimeString1) || dateTimeString1.isEmpty() || dateTimeString2 == null || ("\\N").equals(dateTimeString2) || dateTimeString2.isEmpty()) {
            return null;
        }

        // 缺省值处理
        if (resultFormats.length >= 2) {
            return null;
        } else if (resultFormats.length == 1) {
            if (!resultFormatList.contains(resultFormats[0])) {
                return null;
            }
            resultFormat = resultFormats[0];
        }

        // 日期时间格式判断
        if (dateTimeString1.length() != dateTimeString2.length()) {
            return null;
        } else {
            if (!dateTimeLength.contains(dateTimeString1.length())) {
                return null;
            }
        }
        dateTimeFormat = switch (dateTimeString1.length()) {
            case 19 -> "yyyy-MM-dd HH:mm:ss";
            case 10 -> "yyyy-MM-dd";
            case 8 -> "HH:mm:ss";
            default -> null;
        };


        // 入参处理 计算结果
        if (dateTimeFormat == null) {
            return null;
        } else {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(dateTimeFormat);
                longResult = Math.abs(sdf.parse(dateTimeString1).getTime() - sdf.parse(dateTimeString2).getTime());
            } catch (ParseException e) {
                throw new RuntimeException("DateTime Format Exception", e);
            }
        }

        //结果格式转换
        return switch (resultFormat) {
            case "s" -> (double) longResult / 1000.0;        // 1000
            case "m" -> (double) longResult / 60000.0;       // 1000*60
            case "h" -> (double) longResult / 3600000.0;     // 1000*60*60
            case "d" -> (double) longResult / 86400000.0;    // 1000*60*60*24
            case "w" -> (double) longResult / 604800000.0;   // 1000*60*60*24*7
            case "M" -> (double) longResult / 2592000000.0;  // 1000*60*60*24*30
            case "y" -> (double) longResult / 31536000000.0; // 1000*60*60*24*365
            default -> null;
        };
    }
}
