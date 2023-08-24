package com.hive.udf;
/*
  author       uxbenkel@Github
  createTime   2022-12-06 (1.0) 新建程序
  modifyTime
  funtion      提供类似Oracle的decode函数的转码功能 支持多个码值同时转换
 */

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveDecode extends UDF {
    public HiveDecode() {
    }

    // para1 需要转码的字段
    // para2 源码值,使用逗号分隔
    // para3 目标值,使用逗号分隔
    // para4 缺省值,可选 默认为NULL;如果为self,则保留源码
    public String evaluate(String sourceColumns, String sourceCodes, String targetValues, String... defaultValues) {

        // 设定返回值和缺省值
        StringBuilder targetColumns = null;
        String targetColumn = null;
        String defaultValue = null;

        // 入参空置处理
        if (sourceCodes == null || targetValues == null || ("\\N").equals(sourceCodes) || ("\\N").equals(targetValues) || sourceCodes.isEmpty() || targetValues.isEmpty()) {
            return null;
        }
        // 缺省值处理
        if (defaultValues.length >= 2) {
            return null;
        } else if (defaultValues.length == 1) {
            defaultValue = defaultValues[0];
        }

        // 入参空值处理
        if (sourceColumns == null || ("\\N").equals(sourceColumns) || sourceColumns.isEmpty()) {
            if ("self".equals(defaultValue)) {
                defaultValue = null;
            }
            return defaultValue;
        }

        // 入参处理 转换为数组
        String[] arraySourceColumns = sourceColumns.split(",");
        String[] arrarySourceCodes = sourceCodes.split(",");
        String[] arrayTargetValues = targetValues.split(",");

        // 转码操作
        int j = 0;
        for (String sourceColumn : arraySourceColumns) {
            int i = 0;
            while (i < arrarySourceCodes.length) {
                if (arrarySourceCodes[i].equals(sourceColumn)) {
                    targetColumn = arrayTargetValues[i];
                    break;
                }
                if ("self".equals(defaultValue)) {
                    targetColumn = sourceColumn;
                } else {
                    targetColumn = defaultValue;
                }
                i++;
            }

            // 拼接结果
            if (targetColumn != null) {
                if (j == 0) {
                    targetColumns = new StringBuilder(targetColumn);
                } else {
                    assert targetColumns != null;
                    targetColumns.append(",").append(targetColumn);
                }
            }
            j++;
        }

        // 返回结果
        if (targetColumns == null) {
            return null;
        } else {
            return targetColumns.toString();
        }
    }
}
