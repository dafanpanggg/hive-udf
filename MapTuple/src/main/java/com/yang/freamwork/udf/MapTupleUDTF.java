package com.yang.freamwork.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 按传入的key解析map字段函数，比get_json_object、json_tuple效率高
 * 例：select map_tuple(feature_map, 'x', 'y', 'z') from xxx
 * 返回：  x   y   z
 * 1   2   3
 * 4   0   0
 *
 * @author yangfan
 * @version 1.0.0
 * @since 2020/11/11
 */
@SuppressWarnings("unused")
public class MapTupleUDTF extends GenericUDTF {

    private static MapObjectInspector mapOI = null;
    private static PrimitiveObjectInspector mapKeyOI = null;
    private static PrimitiveObjectInspector mapValueOI = null;
    private static final StringObjectInspector stringOI =
            PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    /**
     * 为了兼容spark2.1、2.3，这里重写过时的initialize方法
     *
     * @param objectInspectors objectInspectors
     * @return StructObjectInspector
     * @throws UDFArgumentException ex
     */
    @SuppressWarnings("deprecation")
    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors)
            throws UDFArgumentException {
        int length = objectInspectors.length;
        if (length < 2) {
            throw new UDFArgumentException("MapTupleUDTF() takes at least 2 arguments");
        }

        if (objectInspectors[0].getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentException("MapTupleUDTF() first param mast be a `map` type");
        }
        for (int i = 1; i < length; i++) {
            if (!(objectInspectors[i] instanceof StringObjectInspector)) {
                throw new UDFArgumentException(
                        "MapTupleUDTF() other params mast be `string` type");
            }
        }

        mapOI = (MapObjectInspector) objectInspectors[0];
        mapKeyOI = (PrimitiveObjectInspector) mapOI.getMapKeyObjectInspector();
        mapValueOI = (PrimitiveObjectInspector) mapOI.getMapValueObjectInspector();

        List<String> fieldNames = new ArrayList<>(length - 1);
        List<ObjectInspector> fieldOIs = new ArrayList<>(length - 1);
        for (int i = 1; i < length; i++) {
            fieldNames.add("col" + i);
            fieldOIs.add(stringOI);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String[] result = new String[objects.length - 1];
        Map<?, ?> map = mapOI.getMap(objects[0]);
        Map<String, String> strMap = (map == null || map.isEmpty()) ?
                null : toStringMap(map, mapKeyOI, mapValueOI);
        for (int i = 1; i < objects.length; i++) {
            String k = stringOI.getPrimitiveJavaObject(objects[i]);
            result[i - 1] = strMap == null ? null : strMap.getOrDefault(k, null);
        }
        forward(result);
    }

    private static Map<String, String> toStringMap(@Nonnull final Map<?, ?> map,
                                                   @Nonnull final PrimitiveObjectInspector keyOI,
                                                   @Nonnull final PrimitiveObjectInspector valueOI) {
        final Map<String, String> result = new HashMap<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String k = PrimitiveObjectInspectorUtils.getString(entry.getKey(), keyOI);
            String v = PrimitiveObjectInspectorUtils.getString(entry.getValue(), valueOI);
            result.put(k, v);
        }
        return result;
    }

    @Override
    public void close() {

    }
}
