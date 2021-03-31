package com.yang.freamwork.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 将其他类型的数据转换成json，较开源的to_json函数，
 * 增加了对复杂类型的支持，如：Array<Array[Double]>等
 *
 * @author yangfan
 * @version 1.0.0
 * @since 2021/3/26
 */
@SuppressWarnings("unused")
public class ToJsonUDF extends GenericUDF {
    private InspectorHandle insHandle;
    private Boolean convertFlag = Boolean.FALSE;
    private JsonFactory jsonFactory;

    private interface InspectorHandle {
        void generateJson(JsonGenerator gen, Object obj) throws IOException;
    }

    private static String ToCamelCase(String underscore) {
        StringBuilder sb = new StringBuilder();
        String[] splArr = underscore.toLowerCase().split("_");
        sb.append(splArr[0]);
        for (int i = 1; i < splArr.length; ++i) {
            String word = splArr[i];
            char firstChar = word.charAt(0);
            if (firstChar >= 'a' && firstChar <= 'z') {
                sb.append((char) (word.charAt(0) + 'A' - 'a'));
                sb.append(word.substring(1));
            } else {
                sb.append(word);
            }

        }
        return sb.toString();
    }

    private class MapInspectorHandle implements InspectorHandle {
        private MapObjectInspector mapInspector;
        private StringObjectInspector keyObjectInspector;
        private InspectorHandle valueInspector;

        MapInspectorHandle(MapObjectInspector mIns) throws UDFArgumentException {
            mapInspector = mIns;
            try {
                keyObjectInspector = (StringObjectInspector) mIns.getMapKeyObjectInspector();
            } catch (ClassCastException castExc) {
                throw new UDFArgumentException("Only Maps with strings as keys can be converted to valid JSON");
            }
            valueInspector = GenerateInspectorHandle(mIns.getMapValueObjectInspector());
        }

        @SuppressWarnings("unchecked")
        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartObject();
                Map map = mapInspector.getMap(obj);
                for (Map.Entry entry : (Iterable<Map.Entry>) map.entrySet()) {
                    String keyJson = keyObjectInspector.getPrimitiveJavaObject(entry.getKey());
                    if (convertFlag) {
                        gen.writeFieldName(ToCamelCase(keyJson));
                    } else {
                        gen.writeFieldName(keyJson);
                    }
                    valueInspector.generateJson(gen, entry.getValue());
                }
                gen.writeEndObject();
            }
        }

    }

    private class StructInspectorHandle implements InspectorHandle {
        private StructObjectInspector structInspector;
        private List<String> fieldNames;
        private List<InspectorHandle> fieldInspectorHandles;

        StructInspectorHandle(StructObjectInspector ins) throws UDFArgumentException {
            structInspector = ins;
            List<? extends StructField> fieldList = ins.getAllStructFieldRefs();
            this.fieldNames = new ArrayList<>();
            this.fieldInspectorHandles = new ArrayList<>();
            for (StructField sf : fieldList) {
                fieldNames.add(sf.getFieldName());
                fieldInspectorHandles.add(GenerateInspectorHandle(sf.getFieldObjectInspector()));
            }
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartObject();
                List structObs = structInspector.getStructFieldsDataAsList(obj);

                for (int i = 0; i < fieldNames.size(); ++i) {
                    String fieldName = fieldNames.get(i);
                    if (convertFlag) {
                        gen.writeFieldName(ToCamelCase(fieldName));
                    } else {
                        gen.writeFieldName(fieldName);
                    }
                    fieldInspectorHandles.get(i).generateJson(gen, structObs.get(i));
                }
                gen.writeEndObject();
            }
        }
    }

    private class ArrayInspectorHandle implements InspectorHandle {
        private ListObjectInspector arrayInspector;
        private InspectorHandle valueInspector;

        ArrayInspectorHandle(ListObjectInspector lIns) throws UDFArgumentException {
            arrayInspector = lIns;
            valueInspector = GenerateInspectorHandle(arrayInspector.getListElementObjectInspector());
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartArray();
                List list = arrayInspector.getList(obj);
                for (Object listObj : list) {
                    valueInspector.generateJson(gen, listObj);
                }
                gen.writeEndArray();
            }
        }
    }

    private class StringInspectorHandle implements InspectorHandle {
        private StringObjectInspector strInspector;

        StringInspectorHandle(StringObjectInspector ins) {
            strInspector = ins;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                String str = strInspector.getPrimitiveJavaObject(obj);
                gen.writeString(str);
            }
        }
    }

    private class IntInspectorHandle implements InspectorHandle {
        private IntObjectInspector intInspector;

        IntInspectorHandle(IntObjectInspector ins) {
            intInspector = ins;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null)
                gen.writeNull();
            else {
                int num = intInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class DoubleInspectorHandle implements InspectorHandle {
        private DoubleObjectInspector dblInspector;

        DoubleInspectorHandle(DoubleObjectInspector ins) {
            dblInspector = ins;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                double num = dblInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class LongInspectorHandle implements InspectorHandle {
        private LongObjectInspector longInspector;

        LongInspectorHandle(LongObjectInspector ins) {
            longInspector = ins;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                long num = longInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class ShortInspectorHandle implements InspectorHandle {
        private ShortObjectInspector shortInspector;

        ShortInspectorHandle(ShortObjectInspector ins) {
            shortInspector = ins;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                short num = shortInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class ByteInspectorHandle implements InspectorHandle {
        private ByteObjectInspector byteInspector;

        ByteInspectorHandle(ByteObjectInspector ins) {
            byteInspector = ins;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                byte num = byteInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class FloatInspectorHandle implements InspectorHandle {
        private FloatObjectInspector floatInspector;

        FloatInspectorHandle(FloatObjectInspector ins) {
            floatInspector = ins;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                float num = floatInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class BooleanInspectorHandle implements InspectorHandle {
        private BooleanObjectInspector boolInspector;

        BooleanInspectorHandle(BooleanObjectInspector ins) {
            boolInspector = ins;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                boolean tf = boolInspector.get(obj);
                gen.writeBoolean(tf);
            }
        }
    }

    private class BinaryInspectorHandle implements InspectorHandle {
        private BinaryObjectInspector binaryInspector;

        BinaryInspectorHandle(BinaryObjectInspector ins) {
            binaryInspector = ins;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                byte[] bytes = binaryInspector.getPrimitiveJavaObject(obj);
                gen.writeBinary(bytes);
            }
        }
    }

    private class TimestampInspectorHandle implements InspectorHandle {
        private TimestampObjectInspector timestampInspector;
        private DateTimeFormatter isoFormatter = ISODateTimeFormat.dateTimeNoMillis();

        TimestampInspectorHandle(TimestampObjectInspector ins) {
            timestampInspector = ins;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                Timestamp timestamp = timestampInspector.getPrimitiveJavaObject(obj);
                String timeStr = isoFormatter.print(timestamp.getTime());
                gen.writeString(timeStr);
            }
        }
    }

    private InspectorHandle GenerateInspectorHandle(ObjectInspector ins) throws UDFArgumentException {
        Category cat = ins.getCategory();
        if (cat == Category.MAP) {
            return new MapInspectorHandle((MapObjectInspector) ins);
        } else if (cat == Category.LIST) {
            return new ArrayInspectorHandle((ListObjectInspector) ins);
        } else if (cat == Category.STRUCT) {
            return new StructInspectorHandle((StructObjectInspector) ins);
        } else if (cat == Category.PRIMITIVE) {
            PrimitiveObjectInspector primIns = (PrimitiveObjectInspector) ins;
            PrimitiveCategory primCat = primIns.getPrimitiveCategory();
            if (primCat == PrimitiveCategory.STRING) {
                return new StringInspectorHandle((StringObjectInspector) primIns);
            } else if (primCat == PrimitiveCategory.INT) {
                return new IntInspectorHandle((IntObjectInspector) primIns);
            } else if (primCat == PrimitiveCategory.LONG) {
                return new LongInspectorHandle((LongObjectInspector) primIns);
            } else if (primCat == PrimitiveCategory.SHORT) {
                return new ShortInspectorHandle((ShortObjectInspector) primIns);
            } else if (primCat == PrimitiveCategory.BOOLEAN) {
                return new BooleanInspectorHandle((BooleanObjectInspector) primIns);
            } else if (primCat == PrimitiveCategory.FLOAT) {
                return new FloatInspectorHandle((FloatObjectInspector) primIns);
            } else if (primCat == PrimitiveCategory.DOUBLE) {
                return new DoubleInspectorHandle((DoubleObjectInspector) primIns);
            } else if (primCat == PrimitiveCategory.BYTE) {
                return new ByteInspectorHandle((ByteObjectInspector) primIns);
            } else if (primCat == PrimitiveCategory.BINARY) {
                return new BinaryInspectorHandle((BinaryObjectInspector) primIns);
            } else if (primCat == PrimitiveCategory.TIMESTAMP) {
                return new TimestampInspectorHandle((TimestampObjectInspector) primIns);
            }
        }
        throw new UDFArgumentException("Don't know how to handle object inspector " + ins);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 1 && args.length != 2) {
            throw new UDFArgumentException(" ToJson takes an object as an argument, and an optional to_camel_case flag");
        }
        ObjectInspector oi = args[0];
        insHandle = GenerateInspectorHandle(oi);

        if (args.length == 2) {
            ObjectInspector flagIns = args[1];
            if (flagIns.getCategory() != Category.PRIMITIVE
                    || ((PrimitiveObjectInspector) flagIns).getPrimitiveCategory()
                    != PrimitiveCategory.BOOLEAN
                    || !(flagIns instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException(" ToJson takes an object as an argument, and an optional to_camel_case flag");
            }
            WritableConstantBooleanObjectInspector constIns = (WritableConstantBooleanObjectInspector) flagIns;
            convertFlag = constIns.getWritableConstantValue().get();
        }

        jsonFactory = new JsonFactory();

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator gen = jsonFactory.createJsonGenerator(writer);
            insHandle.generateJson(gen, args[0].get());
            gen.close();
            writer.close();
            return writer.toString();
        } catch (IOException io) {
            throw new HiveException(io);
        }
    }

    @Override
    public String getDisplayString(String[] args) {
        return "to_json(" + args[0] + ")";
    }

    public static void main(String[] args) throws Exception {
        ToJsonUDF udf = new ToJsonUDF();

        ObjectInspector doubleOi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ObjectInspector listOi1 = ObjectInspectorFactory.getStandardConstantListObjectInspector(doubleOi, new ArrayList<Double>());
        ObjectInspector listOi2 = ObjectInspectorFactory.getStandardConstantListObjectInspector(listOi1, new ArrayList<ArrayList<Double>>());
        udf.initialize(new ObjectInspector[]{listOi2});

        List<Double> arr1 = Arrays.asList(1d, 2d);
        List<Double> arr2 = Arrays.asList(1d, 2d);
        List<List<Double>> arr3 = Arrays.asList(arr1, arr2);
        Object result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(arr3)});
        System.out.println(result);
    }
}
