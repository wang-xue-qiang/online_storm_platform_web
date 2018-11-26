package com.zkh.utils;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
public class DateUtils {
	/**
	 * 计算日期
	 * @param date    日期字符串
	 * @param pattern 日期格式化
	 * @param step    计算量
	 * @return
	 */
	public static String getDate(String date,String pattern,int step){
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Calendar cal  = Calendar.getInstance();
		if(date != null){
			try {
				cal.setTime(sdf.parse(date));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		cal.add(Calendar.DATE, step);
		return sdf.format(cal.getTime());
	}
	
	public static String getToday(String pattern){
		if(null == pattern){pattern ="yyyy-MM-dd";}
		SimpleDateFormat formatter = new SimpleDateFormat(pattern);
		String  today = formatter.format(new Date());
		return today;
	}
	public static void main(String[] args) throws Exception {
		System.out.println(DateUtils.getDate(null, "yyyy-MM-dd", -1));
		System.out.println(DateUtils.getToday(null));
		System.out.println( new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").parse("2018-11-08 10:17:57:055").getTime());
	}
}
