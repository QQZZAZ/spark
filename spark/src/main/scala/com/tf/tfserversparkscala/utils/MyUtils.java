package com.tf.tfserversparkscala.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

/**
 * 工具类
 */
public class MyUtils {
    //将string转换成list
    public static List<String> stringToList(String tmp) {
        List<String> list = new ArrayList<String>();

        if ((tmp != null) && !tmp.equals("")) {
            String[] fields = tmp.split(" ");

            for (int j = 0; j < fields.length; j++) {
                if (fields[j].matches("[\\u4e00-\\u9fa5]{1,}")) {
                    list.add(fields[j]);
                } else {
                    continue;
                }
            }
        }

        return list;
    }

    //获取地区编码
    public static String getRecordCode(String placeName) {
        String recordCode = "Undisclosed";

        if ((placeName != null) && !placeName.equals("")) {
            if (placeName.equals("京") || placeName.equals("北京市")) {
                recordCode = "110000";
            } else if (placeName.equals("津") || placeName.equals("天津市")) {
                recordCode = "120000";
            } else if (placeName.equals("冀") || placeName.equals("河北省")) {
                recordCode = "130000";
            } else if (placeName.equals("晋") || placeName.equals("山西省")) {
                recordCode = "140000";
            } else if (placeName.equals("蒙") || placeName.equals("内蒙古自治区")) {
                recordCode = "150000";
            } else if (placeName.equals("辽") || placeName.equals("辽宁省")) {
                recordCode = "210000";
            } else if (placeName.equals("吉") || placeName.equals("吉林省")) {
                recordCode = "220000";
            } else if (placeName.equals("黑") || placeName.equals("黑龙江省")) {
                recordCode = "230000";
            } else if (placeName.equals("沪") || placeName.equals("上海市")) {
                recordCode = "310000";
            } else if (placeName.equals("苏") || placeName.equals("江苏省")) {
                recordCode = "320000";
            } else if (placeName.equals("浙") || placeName.equals("浙江省")) {
                recordCode = "330000";
            } else if (placeName.equals("皖") || placeName.equals("安徽省")) {
                recordCode = "340000";
            } else if (placeName.equals("闽") || placeName.equals("福建省")) {
                recordCode = "350000";
            } else if (placeName.equals("赣") || placeName.equals("江西省")) {
                recordCode = "360000";
            } else if (placeName.equals("鲁") || placeName.equals("山东省")) {
                recordCode = "370000";
            } else if (placeName.equals("豫") || placeName.equals("河南省")) {
                recordCode = "410000";
            } else if (placeName.equals("鄂") || placeName.equals("湖北省")) {
                recordCode = "420000";
            } else if (placeName.equals("湘") || placeName.equals("湖南省")) {
                recordCode = "430000";
            } else if (placeName.equals("粤") || placeName.equals("广东省")) {
                recordCode = "440000";
            } else if (placeName.equals("桂") || placeName.equals("广西壮族自治区")) {
                recordCode = "450000";
            } else if (placeName.equals("琼") || placeName.equals("海南省")) {
                recordCode = "460000";
            } else if (placeName.equals("渝") || placeName.equals("重庆市")) {
                recordCode = "500000";
            } else if (placeName.equals("川") || placeName.equals("蜀") ||
                    placeName.equals("四川省")) {
                recordCode = "510000";
            } else if (placeName.equals("黔") || placeName.equals("贵") ||
                    placeName.equals("贵州省")) {
                recordCode = "520000";
            } else if (placeName.equals("滇") || placeName.equals("云") ||
                    placeName.equals("云南省")) {
                recordCode = "530000";
            } else if (placeName.equals("藏") || placeName.equals("西藏自治区")) {
                recordCode = "540000";
            } else if (placeName.equals("陕") || placeName.equals("秦") ||
                    placeName.equals("陕西省")) {
                recordCode = "610000";
            } else if (placeName.equals("甘") || placeName.equals("陇") ||
                    placeName.equals("甘肃省")) {
                recordCode = "620000";
            } else if (placeName.equals("青") || placeName.equals("青海省")) {
                recordCode = "630000";
            } else if (placeName.equals("宁") || placeName.equals("宁夏回族自治区")) {
                recordCode = "640000";
            } else if (placeName.equals("新") || placeName.equals("新疆维吾尔自治区")) {
                recordCode = "650000";
            } else if (placeName.equals("台") || placeName.equals("台湾省")) {
                recordCode = "710000";
            } else if (placeName.equals("港") || placeName.equals("香港特别行政区")) {
                recordCode = "810000";
            } else if (placeName.equals("澳") || placeName.equals("澳门特别行政区")) {
                recordCode = "820000";
            }
        }

        return recordCode;
    }


    //文本是否含有图片
    public static boolean hasEmoji(String content){
        Pattern pattern = Pattern.compile("[\\uD83C\\uDC04-\\uD83C\\uDE1A]|[\\uD83D\\uDC66-\\uD83D\\uDC69]|[\\uD83D\\uDC66\\uD83C\\uDFFB-\\uD83D\\uDC69\\uD83C\\uDFFF]|[\\uD83D\\uDE45\\uD83C\\uDFFB-\\uD83D\\uDE4F\\uD83C\\uDFFF]|[\\uD83C\\uDC00-\\uD83D\\uDFFF]|[\\uD83E\\uDD10-\\uD83E\\uDDC0]|[\\uD83D\\uDE00-\\uD83D\\uDE4F]|[\\uD83D\\uDE80-\\uD83D\\uDEF6]");
        Matcher matcher = pattern.matcher(content);
        if(matcher.find()){
            return true;
        }
        return false;
    }


}
