package com.yang.freamwork.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
  * 通过经纬度获取距离(单位：米)
  *
  * @author yangfan
  * @since 2021/3/30
  * @version 1.0.0
  */
@SuppressWarnings("unused")
public class LonLatDistanceUDF extends UDF {

    private static final double EARTH_RADIUS = 6378.137D;

    private double rad(double d) {
        return d * Math.PI / 180D;
    }

    public Double evaluate(Double lon1, Double lat1, Double lon2, Double lat2) {
        try {
            double radLat1 = rad(lat1);
            double radLat2 = rad(lat2);
            double a = radLat1 - radLat2;
            double b = rad(lon1) - rad(lon2);
            double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) +
                    Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
            s = s * EARTH_RADIUS;
            s = s * Math.round(10000d) / 10000d;
            s = s * 1000;
            return s;
        } catch (Exception e) {
            return null;
        }
    }

    public Double evaluate(String lon1, String lat1, String lon2, String lat2) {
        try {
            return evaluate(Double.valueOf(lon1), Double.valueOf(lat1),
                    Double.valueOf(lon2), Double.valueOf(lat2));
        } catch (Exception e) {
            return null;
        }
    }

    public Double evaluate(String lonLatStr1, String lonLatStr2) {
        try {
            String[] lonLat1 = lonLatStr1.split(",");
            String[] lonLat2 = lonLatStr2.split(",");
            return evaluate(Double.valueOf(lonLat1[0]), Double.valueOf(lonLat1[1]),
                    Double.valueOf(lonLat2[0]), Double.valueOf(lonLat2[1]));
        } catch (Exception e) {
            return null;
        }
    }

    public static void main(String[] args) {
        LonLatDistanceUDF udf = new LonLatDistanceUDF();
        Double result1 = udf.evaluate(110.312, 30.132, 123.42, 32.011);
        Double result2 = udf.evaluate("110.312", "30.132", "123.42", "32.011");
        Double result3 = udf.evaluate("110.312,30.132", "123.42,32.011");
        Double result4 = udf.evaluate(null, 30.132, 123.42, 32.011);
        System.out.println(result1);
        System.out.println(result2);
        System.out.println(result3);
        System.out.println(result4);
    }
}
