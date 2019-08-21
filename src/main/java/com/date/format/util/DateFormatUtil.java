package com.date.format.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.LocalDate;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author mingliang.ge
 * @date 18-7-25
 */
@Description(name = "running_dates"
        , value = "_FUNC_(date1,date2) - return a running dates list from date1 to date2 with a singer format like yyyy-mm-dd."
        , extended = "Example:\n > select _FUNC_(date1_string,date2_string) from src;")
public class DateFormatUtil extends UDF {
    public final static DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    public final static DateTimeFormatter OUT_DATE_FORMATTER = DateTimeFormat.forPattern("yyyyMMdd");
    public final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    public DateFormatUtil() {

    }

    public Text evaluate(Text dateString1, Text dateString2) {
        if (dateString1 == null || dateString2 == null) {
            return null;
        }
        Text result = new Text();
        ArrayList<String> rs = new ArrayList<>();
        try {
            LocalDate date1 = LocalDate.parse(dateString1.toString(), DEFAULT_DATE_FORMATTER);
            LocalDate date2 = LocalDate.parse(dateString2.toString(), DEFAULT_DATE_FORMATTER);

            Period p = new Period(date1, date2, PeriodType.days());
            int diff = p.getDays();
            for (int i = 0; i <= diff; i++) {
                rs.add(date1.plusDays(i).toString(OUT_DATE_FORMATTER));
            }
            result.set(String.join(",", rs));
            return result;
        } catch (Exception e) {
            return null;
        }
    }

    public Text evaluate(Text dateString){
        Calendar cale = Calendar.getInstance();
        Date date = null;
        try {
            date = format.parse(dateString.toString());
            cale.setTime(date);
            return format(cale);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Text evaluate() {
        Calendar cale = Calendar.getInstance();
        return format(cale);
    }

    public Text format(Calendar cale){
        Text text1 = new Text();
        text1.set(getFirstDayOfMonth(cale,true));
        Text text2 = new Text();
        text2.set(getFirstDayOfMonth(cale,false));
        return evaluate(text1,text2);
    }

    public static String getFirstDayOfMonth(Calendar cale,boolean isFirst){
        if(isFirst){
            cale.add(Calendar.MONTH, 0);
            cale.set(Calendar.DAY_OF_MONTH, 1);
        }else{
            cale.add(Calendar.MONTH, 1);
            cale.set(Calendar.DAY_OF_MONTH, 0);
        }
        return format.format(cale.getTime());
    }

}
