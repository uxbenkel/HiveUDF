package com.hive.udf;
/*
  author       uxbenkel@Github
  createTime   2022-08-01 (1.0) 新建程序
  modifyTime   2022-08-08 (1.1) 入参空置处理
  funtion      判断给定的时间是否处于一组时间区间内
 */

import org.apache.hadoop.hive.ql.exec.UDF;

public class TimeRangeMatch extends UDF {
    public TimeRangeMatch() {
    }

    // para1 需要判断的时间
    // para2 需要判断的时间区间 格式 开始时间1,结束时间1|开始时间2,结束时间2|...
    public String evaluate(String time, String timeRanges) {
        // 设定返回值
        String matchResult = "N";
        // 入参空值处理
        if (time == null || timeRanges == null || ("\\N").equals(time) || ("\\N").equals(timeRanges) || time.isEmpty() || timeRanges.isEmpty()) {
            return null;
        }
        String[] arrayTimeRanges = timeRanges.split("\\|");
        for (String timeRange : arrayTimeRanges) {
            String startTime = timeRange.split(",")[0];
            String endTime = timeRange.split(",")[1];
            // 如果 时间 >= 开始时间 并且 时间 <= 结束时间 则将结果赋为 是

            if (time.compareTo(startTime) >= 0 && time.compareTo(endTime) <= 0) {
                matchResult = "Y";
                break;
            }
        }
        return matchResult;
    }
}