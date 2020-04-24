package com.tf.tfserversparkscala.utils.dateUtils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间操作工具类
 */
public class DateUtils {
    //将string时间格式转换成Timestamp
    public static Timestamp toTimestamp(String time) {
        Timestamp ts = Timestamp.valueOf(time);

        return ts;
    }

    //返回指定日期的前一天零点的long类型时间戳
    public static long startTimeToLong_yesterday(String time) {
        String[] fields = time.split(" ");
        long startTimeLong = timeTransformation(fields[0]);

        return startTimeLong - 86400000;
    }

    //返回指定日期的前一天23点的long类型时间戳
    public static long endTimeToLong_yesterday(String time) {
        String[] fields = time.split(" ");
        String timetmp = fields[0] + " 23:59:59";
        long startTimeLong = timeTransformation(timetmp);

        return startTimeLong - 86400000;
    }

    //返回指定日期的当天零点的long类型时间戳
    public static long startTimeToLong_day(String time) {
        String[] fields = time.split(" ");
        long startTimeLong = timeTransformation(fields[0]);

        return startTimeLong;
    }

    //返回指定日期的三天前的long类型时间戳
    public static long startTimeToLong_3day(String time) {
        String[] fields = time.split(" ");
        long startTimeLong = timeTransformation(fields[0]);

        return startTimeLong - 259200000;
    }

    //返回指定日期的上周的最后一天
    public static long getLastDayOfLastWeek(String time) {
        String[] fields = time.split(" ");
        String endTime = fields[0] + " " + "23:59";
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        long endTimeLong = 0;

        try {
            Date date = sf.parse(endTime);
            Calendar calendar = Calendar.getInstance();
            calendar.setFirstDayOfWeek(calendar.MONTH); //将每周第一天设为星期一，默认是星期天
            calendar.setTime(date);
            calendar.add(Calendar.DATE, -1 * 7);
            calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);

            Date endTimeDate = calendar.getTime();
            endTimeLong = endTimeDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return endTimeLong;
    }

    //返回指定日期的上周的第一天
    public static long getFirstDayOfLastWeek(String time) {
        String[] fields = time.split(" ");
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
        long startTimeLong = 0;

        try {
            Date date = sf.parse(fields[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setFirstDayOfWeek(calendar.MONTH); //将每周第一天设为星期一，默认是星期天
            calendar.setTime(date);
            calendar.add(Calendar.DATE, -1 * 7);
            calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

            Date endTimeDate = calendar.getTime();
            startTimeLong = endTimeDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return startTimeLong;
    }

    //返回指定日期的上个月的最后一天
    public static long getLastDayOfLastMonth(String time) {
        String[] fields = time.split(" ");
        String endTime = fields[0] + " " + "23:59";
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        long endTimeLong = 0;

        try {
            Date date = sf.parse(endTime);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.set(calendar.get(Calendar.YEAR),
                    calendar.get(Calendar.MONTH) - 1, 1);
            calendar.roll(Calendar.DATE, -1);

            Date endTimeDate = calendar.getTime();
            endTimeLong = endTimeDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return endTimeLong;
    }

    //返回指定日期的上个月的第一天
    public static long getFirstDayOfLastMonth(String time) {
        String[] fields = time.split(" ");
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
        long startTimeLong = 0;

        try {
            Date date = sf.parse(fields[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.set(calendar.get(Calendar.YEAR),
                    calendar.get(Calendar.MONTH) - 1, 1);
            calendar.roll(Calendar.DATE, 0);

            Date endTimeDate = calendar.getTime();
            startTimeLong = endTimeDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return startTimeLong;
    }

    //根据传入的时间计算前一年的开始时间
    public static long getFirstDayOfLastYear(String time) {
        String[] fields = time.split(" ");
        String year = fields[0].split("\\-")[0];
        String startTime = Integer.parseInt(year) - 1 + "-" + "01-01";
        long startTimeLong = timeTransformation(startTime);

        return startTimeLong;
    }

    //根据传入的时间计算前一年的结束时间
    public static long getLastDayOfLastYear(String time) {
        String[] fields = time.split(" ");
        String year = fields[0].split("\\-")[0];
        String endTime = Integer.parseInt(year) - 1 + "-" + "12-30 23:59";
        long endTimeLong = timeTransformation(endTime);

        return endTimeLong;
    }

    //将string类型的时间转换long类型时间戳
    public static long timeTransformation(String time) {
        long timeStemp = 0;

        if ((time != null) && !time.equals("")) {
            if (time.matches(
                    "\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}")) {
                SimpleDateFormat sf = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss");
                Date date = null;

                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                timeStemp = date.getTime();

                return timeStemp;
            } else if (time.matches(
                    "\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                Date date = null;

                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                timeStemp = date.getTime();

                return timeStemp;
            } else if (time.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH");
                Date date = null;

                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                timeStemp = date.getTime();

                return timeStemp;
            } else if (time.matches("\\d{4}-\\d{1,2}-\\d{1,2}")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
                Date date = null;

                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                timeStemp = date.getTime();

                return timeStemp;
            } else if (time.matches(
                    "\\d{4}年\\d{1,2}月\\d{1,2}日\\d{1,2}:\\d{1,2}:\\d{1,2}")) {
                SimpleDateFormat sf = new SimpleDateFormat(
                        "yyyy年MM月dd日HH:mm:ss");
                Date date = null;

                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                timeStemp = date.getTime();

                return timeStemp;
            } else if (time.matches(
                    "\\d{4}年\\d{1,2}月\\d{1,2}日\\d{1,2}:\\d{1,2}")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy年MM月dd日HH:mm");
                Date date = null;

                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                timeStemp = date.getTime();

                return timeStemp;
            } else if (time.matches("\\d{4}年\\d{1,2}月\\d{1,2}日")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy年MM月dd日");
                Date date = null;

                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                timeStemp = date.getTime();

                return timeStemp;
            } else if (time.matches(
                    "\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{1,2}:\\d{1,2}Z")) {
                SimpleDateFormat sf = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss");
                Date date = null;

                try {
                    date = sf.parse(time.replace("T", " ").replace("Z", ""));
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                timeStemp = date.getTime();

                return timeStemp;
            } else if (time.matches("\\d{0,}")) {
                if (time.length() == 10) {
                    timeStemp = Long.parseLong(time) * 1000;
                } else if (time.length() == 13) {
                    timeStemp = Long.parseLong(time);
                }

                return timeStemp;
            } else if (time.matches(
                    "[a-zA-Z]{3}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}")) {
                String[] fields = time.split(" ");
                String time_tmp = fields[1];
                String date_tmp = fields[0];
                String[] fields_date = date_tmp.split("-");
                String year = "20" + fields_date[2];
                String month_tmp = fields_date[0];
                String day = fields_date[1];
                String month = "";

                if (month_tmp.equals("Jan")) {
                    month = "01";
                } else if (month_tmp.equals("Feb")) {
                    month = "02";
                } else if (month_tmp.equals("Mar")) {
                    month = "03";
                } else if (month_tmp.equals("Apr")) {
                    month = "04";
                } else if (month_tmp.equals("May")) {
                    month = "05";
                } else if (month_tmp.equals("Jun")) {
                    month = "06";
                } else if (month_tmp.equals("Jul")) {
                    month = "07";
                } else if (month_tmp.equals("Aug")) {
                    month = "08";
                } else if (month_tmp.equals("Sep")) {
                    month = "09";
                } else if (month_tmp.equals("Oct")) {
                    month = "10";
                } else if (month_tmp.equals("Nov")) {
                    month = "11";
                } else if (month_tmp.equals("Dec")) {
                    month = "12";
                }

                String result_time = year + "-" + month + "-" + day + " " +
                        time_tmp;
                SimpleDateFormat sf = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss");
                Date date = null;

                try {
                    date = sf.parse(result_time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                timeStemp = date.getTime();

                return timeStemp;
            }
        }

        return timeStemp;
    }

    //过滤掉不在某一段时间范围内的记录
    public static boolean timeFilter(long startTime, long time, long endTime) {
        if ((time >= startTime) && (time <= endTime)) {
            return true;
        }
        return false;
    }
}
