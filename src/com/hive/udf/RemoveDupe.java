package com.hive.udf;
/*
  author       uxbenkel@Github
  createTime   2023-06-14 (1.0) 新建程序
  modifyTime
  funtion      提供类似 hive 的 collect_set 函数的数组元素去重功能 出入参皆为 String 外部用 concat_ws 和 split 自行处理数组和字符串间的转换
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class RemoveDupe extends UDF {
    public RemoveDupe() {
    }

    // para1 需要处理的字段
    // para2 分隔符 默认为 , 分号可传 \073 禁止传 \
    public String evaluate(String sourceColumns, String... defaultValues) {

        // 设定返回值和缺省值
        String targetColumn;
        String defaultValue = ",";
        String regexpDelimiter = ",";

        // 入参空值处理
        if (sourceColumns == null || ("\\N").equals(sourceColumns) || ("").equals(sourceColumns)) {
            return null;
        }

        // 正则特殊字符
        String regexpSymbols = "^$*?+-.|";

        //缺省值处理
        if (defaultValues.length >= 2) {
            return null;
        } else if (defaultValues.length == 1) {
            defaultValue = defaultValues[0];
            if ("\073".equals(defaultValues[0])) {
                defaultValue = ";";
                regexpDelimiter = ";";
            } else if (regexpSymbols.contains(defaultValues[0])) {
                regexpDelimiter = "\\" + defaultValue;
            } else {
                regexpDelimiter = defaultValue;
            }
        }

        // 字段分割处理为 list
        List<String> listSourceColumns = Arrays.asList(sourceColumns.split(regexpDelimiter));

        // 将 list 放入 hashset
        HashSet<String> hashsetSourceColumns = new HashSet<>(listSourceColumns);

        // 返回结果 hashset 不会去除空 或者 空字符串 但是总会只保留一个 而且根据自然排序会置于结果起始处
        targetColumn = StringUtils.join(hashsetSourceColumns.toArray(), defaultValue).replaceAll("^" + regexpDelimiter, "");

        return targetColumn;
    }
}
