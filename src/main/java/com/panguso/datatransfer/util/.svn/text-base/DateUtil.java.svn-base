/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liubing
 *
 */
public final class DateUtil {
	/**
	 * 精确到天单位日期格式化符号
	 */
	public static final SimpleDateFormat DAYSDF = new SimpleDateFormat("yyyyMMdd");
	/**
	 * 精确到秒单位的日期格式化符号
	 */
	public static final SimpleDateFormat SECSDF = new SimpleDateFormat("yyyyMMddhhmmss");
	private static final Logger LOG = LoggerFactory.getLogger(DateUtil.class);
	private static String[] unit = {"ms ", "s ", "Mins ", "Hs ", "days "};
	private static long[] ratio = {1000, 60, 60, 24, 1};
	private static final int unitLen = 5; 
	
	/**
	 * 构造函数
	 */
	private DateUtil() {
		
	}
	
	
	/**
	 * @function 获取可读形式的时间间隔
	 * @param timeDiff 时间间隔
	 * @return 可读的时间间隔形式字符串
	 */
	public static String getReadableTime(long timeDiff) {
		if (timeDiff <= 0) {
			return "";
		}
		
		long[] unitNum = new long[5];
		int idx = 0;
		while (idx < unitLen && timeDiff >= ratio[idx]) {
			unitNum[idx] = timeDiff % ratio[idx];
			timeDiff = timeDiff / ratio[idx];
			idx++;
		}
		
		if (idx < unitLen) {
			unitNum[idx] = timeDiff;
		} else {
			unitNum[--idx] = timeDiff;
		}

		
		StringBuilder sb = new StringBuilder();
		while (idx >= 0) {
			if (unitNum[idx] == 0) {
				--idx;
				continue;
			}
			sb.append(unitNum[idx] + unit[idx]);
			--idx;
		}
		return sb.toString();
	}
	
	
	/**
	 * @function 在当天的基础上增加一天
	 * @param day 要增加的当天
	 * @return 增加一天以后的时间字符串
	 */
	public static String addOneDay(String day) {
		if (CommonUtil.isNullOrEmpty(day)) {
			return getToday();
		}
		//设置日历对象，并设置到当天的后一天
	    Date date = null;
	    Calendar calendar = Calendar.getInstance(); 
		try {
			date = DAYSDF.parse(day);
		    calendar.setTime(date); 
		    calendar.add(Calendar.DAY_OF_MONTH, 1); 
		} catch (ParseException e) {
			e.printStackTrace();
		} 
	    String newday = DAYSDF.format(calendar.getTime());
	    LOG.debug("after add one day : " + newday);
	    return newday;
	}
	
	/**
	 * @function 	   在当天的基础上增加N天
	 * @param day 	   添加之前的当天
	 * @param number 要增加的天数
	 * @return 		   增加后的日期
	 */
	public static String addDays(String day, int number) {
		if (CommonUtil.isNullOrEmpty(day)) {
			return getToday();
		}
		 
	    Date date = null;
	    Calendar calendar = Calendar.getInstance(); 
		try {
			date = DAYSDF.parse(day);
		    calendar.setTime(date); 
		    calendar.add(Calendar.DAY_OF_MONTH, number); 
		} catch (ParseException e) {
			e.printStackTrace();
		} 
	    String newday = DAYSDF.format(calendar.getTime());
	    LOG.debug("after add days : " + newday);
	    return newday;
	}
	
	
	/**
	 * @function 获取以yyyyMMdd格式的今天的日期
	 * @return   格式化的今日日期
	 */
	public static String getToday() {
		return DAYSDF.format(new Date());
	}
	
	
	/**
	 * @function 从日期集合中获取最早的时间 返回格式为yyyyMMdd
	 * @param dates 日期集合
	 * @return 日期字符串集合中时间最早的字符串
	 */
	public static String getEarliestDate(Set<String> dates) {
		String earliestDate = getToday();
		if (!CommonUtil.isNullOrEmpty(dates)) {
			for (String date : dates) {
				if (date.compareTo(earliestDate) < 0) {
					earliestDate = date;
				}
			}
		}
		if (earliestDate.length() > 8) {
			earliestDate = earliestDate.substring(0, 8);
		}
		return earliestDate;
	}
	
	/**
	 * @function 获取当前时间下一分钟对应时间的crontab字符串
	 * @return   对应crontab字符串
	 */
	public static String getCrotabOfNextMinute() {
		Calendar cal = Calendar.getInstance();
		
		int day = cal.get(Calendar.DAY_OF_MONTH);
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		int minute = cal.get(Calendar.MINUTE) + 1;
		int second = cal.get(Calendar.SECOND);
		String crontab = second + " " + minute + " " + hour + " " + day + " * ?";
		return crontab;
	}
	
	/**
	 * @function 获取当前时间，精确到秒
	 * @return   精确到秒的当前时间
	 */
	public static String getNow() {
		String now = SECSDF.format(new Date());
		return now;
	}
}
