package com.yang.freamwork.udf;

import com.google.i18n.phonenumbers.Phonenumber;
import com.google.i18n.phonenumbers.geocoding.PhoneNumberOfflineGeocoder;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Locale;

/**
  * 根据手机号获取归属地
  *
  * @author yangfan
  * @since 2021/3/30
  * @version 1.0.0
  */
@SuppressWarnings("unused")
public class PhoneNumberGeoUDF extends GenericUDF {

    private static PhoneNumberOfflineGeocoder geoCoder;
    private static final Phonenumber.PhoneNumber pn;

    static {
        pn = new Phonenumber.PhoneNumber();
        pn.setCountryCode(86);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 1) {
            throw new UDFArgumentException("args length mast be 1 !");
        }
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("args 0 mast be a `PRIMITIVE` type");
        }
        geoCoder = PhoneNumberOfflineGeocoder.getInstance();
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) {
        try {
            pn.setNationalNumber(Long.valueOf(String.valueOf(deferredObjects[0].get())));
            String phone = geoCoder.getDescriptionForNumber(pn, Locale.CHINESE);
            pn.setNationalNumber(0L);
            return null == phone || phone.isEmpty() ? null : phone;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "get_phone_geo(" + strings[0] + ")";
    }

    public static void main(String[] args) throws UDFArgumentException {
        PhoneNumberGeoUDF udf = new PhoneNumberGeoUDF();
        //ObjectInspector[] oi = {PrimitiveObjectInspectorFactory.javaStringObjectInspector};
        ObjectInspector[] oi = {PrimitiveObjectInspectorFactory.javaLongObjectInspector};
        udf.initialize(oi);
        GenericUDF.DeferredObject[] o1 = {new GenericUDF.DeferredJavaObject("18888888888")};
        GenericUDF.DeferredObject[] o2 = {new GenericUDF.DeferredJavaObject("0")};
        GenericUDF.DeferredObject[] o3 = {new GenericUDF.DeferredJavaObject(null)};
        GenericUDF.DeferredObject[] o4 = {new GenericUDF.DeferredJavaObject(18888888888L)};
        GenericUDF.DeferredObject[] o5 = {new GenericUDF.DeferredJavaObject(18888888888D)};
        System.out.println(udf.evaluate(o1));
        System.out.println(udf.evaluate(o2));
        System.out.println(udf.evaluate(o3));
        System.out.println(udf.evaluate(o4));
        System.out.println(udf.evaluate(o5));
    }
}
