package com.zkh.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;

import com.zkh.dao.HbaseDao;
import com.zkh.dao.HbaseDaoImpl;

public class BuquanData {
    public static void main(String[] args) throws Exception {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2018-10-26");
        List<Date> dateMis = getDayMi(date);
        HbaseDao dao = new HbaseDaoImpl();
        List<Put> puts = new ArrayList<Put>();
        int i =3;
        for (Date dateMi : dateMis) {
        	i+=1;
            String rowkey =new SimpleDateFormat("YYYYMMddHHmm").format(dateMi);
            String time_title =new SimpleDateFormat("HH:mm").format(dateMi);
            Put put = new Put(rowkey.getBytes());
    		put.add("cf".getBytes(), "uv".getBytes(), (i+"").getBytes());
    		Put put2 = new Put(rowkey.getBytes());
    		put2.add("cf".getBytes(), "time_title".getBytes(), time_title.getBytes());
     		Put put3 = new Put(rowkey.getBytes());
    		put3.add("cf".getBytes(), "xValue".getBytes(), getXValueStr(dateMi)[1].getBytes());
    		puts.add(put);
    		puts.add(put2);
    		puts.add(put3);
        }
        dao.save(puts, "uv");
    	System.out.println();
    }
    /**
     * 获取某一天的每一分钟
     * @param date
     * @return
     */
    private static List<Date> getDayMi(Date date) {
        Calendar tt = Calendar.getInstance();
        tt.setTime(date);
        Calendar t2 = Calendar.getInstance();
        t2.setTime(date);
        t2.add(Calendar.DAY_OF_MONTH, 1);
        List<Date> dateList = new ArrayList<Date>();
        for (;tt.compareTo(t2)<0; tt.add(Calendar.MINUTE, 1)) {
            dateList.add(tt.getTime());
        }
        return dateList;
    }
	//获取x轴
	public static  String[] getXValueStr(Date date){
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int sec = c.get(Calendar.SECOND);
		int curSecNum = hour*3600+ minute*60+sec;
		Double xValue = (double)curSecNum/3600;
		String[] end = {hour+":"+minute,xValue.toString()};
		return end;
	}
	
	
	
    
}