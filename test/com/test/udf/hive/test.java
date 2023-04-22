package com.test.udf.hive;

import com.hive.udf.HiveDecode;
import com.hive.udf.LinearScoreCalculation;
import com.hive.udf.NearestTimeSelect;
import com.hive.udf.TimeRangeMatch;

public class test {
    public static void main(String[] args) {
        TimeRangeMatch trm = new TimeRangeMatch();
        String result1 = trm.evaluate("2022-09-10", "2021-09-10,2022-10-01|2023-01-01,2023-01-02");
        System.out.println(result1);
        String result2 = trm.evaluate("2022-09-01", "2022-09-10,2022-10-01|2023-01-01,2023-01-02");
        System.out.println(result2);

        NearestTimeSelect nts = new NearestTimeSelect();
        String result3 = nts.evaluate("4", "G", "1,2,3,5,6");
        System.out.println(result3);
        String result4 = nts.evaluate("4", "L", "1,2,3,5,6");
        System.out.println(result4);

        HiveDecode hd = new HiveDecode();
        String result5 = hd.evaluate("4", "1,2,3,4", "A,B,C,D");
        System.out.println(result5);
        String result6 = hd.evaluate("5", "1,2,3,4", "A,B,C,D", "self");
        System.out.println(result6);

        LinearScoreCalculation lsc = new LinearScoreCalculation();
        String result7 = lsc.evaluate("0.25", "A", "0.0,0.1,0.2,0.29,0.4,0.5", "0,60,70,80,90,100");
        System.out.println(result7);
    }
}
