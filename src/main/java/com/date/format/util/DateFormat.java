package com.date.format.util;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateFormat extends GenericUDTF {
    public final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    @Override
    public void process(Object[] args) throws HiveException {
        if(args == null || args.length == 0){
            formatDate();
        }else if(args.length == 1){
            formatDate(args[0].toString());
        }else if(args.length == 2){
            formatDate(args[0].toString(),args[1].toString());
        }
    }

    @Override
    public void close() throws HiveException {

    }

    public void formatDate(){
        Calendar cale = Calendar.getInstance();
        format(cale);
    }

    public void formatDate(String dateString){
        Calendar cale = Calendar.getInstance();
        Date date ;
        try {
            date = format.parse(dateString.toString());
            cale.setTime(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        format(cale);
    }

    public void format(Calendar cale){
        if(cale == null){
            cale = Calendar.getInstance();
        }
        formatDate(DateFormatUtil.getFirstDayOfMonth(cale,true),DateFormatUtil.getFirstDayOfMonth(cale,false));
    }

    public void formatDate(String startTime,String endTime){

    }
}
