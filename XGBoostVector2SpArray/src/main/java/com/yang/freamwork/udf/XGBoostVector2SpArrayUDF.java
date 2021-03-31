package com.yang.freamwork.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * 遍历传入数组，将其转为目标向量数组的稀疏数组（XGBoost模型）
 * 入参：【String, String, String】：待转换数组字符串，目标数组字符串，分隔符
 * 例：select vector_trans("1,6", "1,2,3,4,5,6", ",")
 * 返回值：【Map<Integer, Double>】:目标向量数组的稀疏数组
 * 例：[[-1.0,6.0],[0.0,1.0],[5.0,1.0]]
 *
 * @author yangfan
 * @version 1.0.0
 * @version 1.0.1 增加支持输出数组为稀疏数组的功能
 * @since 2021/3/18
 */
@SuppressWarnings("unused")
public class XGBoostVector2SpArrayUDF extends UDF {

    private Map<String, Double> targetMap;

    public List<List<Double>> evaluate(String str1,
                                       @Nonnull String str2,
                                       @Nonnull String regex) {
        if (null == str1 || str1.length() == 0) {
            return null;
        }
        if (null == targetMap) {
            targetMap = new LinkedHashMap<>();
            for (String s : str2.split(regex)) {
                targetMap.put(s, 0d);
            }
        }
        Map<String, Double> map = new LinkedHashMap<String, Double>() {{
            putAll(targetMap);
        }};
        for (String s : str1.split(regex)) {
            if (map.containsKey(s)) {
                map.put(s, 1d);
            }
        }
        List<List<Double>> sparse = new ArrayList<>();
        Double[] array = map.values().toArray(new Double[targetMap.size()]);
        sparse.add(Arrays.asList(-1d, (double) array.length));
        for (int i = 0; i < array.length; i++) {
            if (array[i] != 0d) {
                sparse.add(Arrays.asList((double) i, array[i]));
            }
        }
        return sparse;
    }

    public static void main(String[] args) {
        XGBoostVector2SpArrayUDF udf = new XGBoostVector2SpArrayUDF();
        List<List<Double>> result1 = udf.evaluate("1,6", "1,2,3,4,5,6", ",");
        List<List<Double>> result2 = udf.evaluate(null, "1,2,3,4,5,6", ",");
        System.out.println(result1);
        System.out.println(result2);
    }
}