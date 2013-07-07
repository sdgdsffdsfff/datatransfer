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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;

/**
 * @author liubing
 *
 */
public final class CommonUtil {
	
	private static final Logger LOG = LoggerFactory.getLogger(CommonUtil.class); 
	
	private CommonUtil() {
		
	}
	
	/**
	 * @function  判断一个字符串是否为null或空值
	 * @param str 待判断的字符串
	 * @return	     判断结果
	 */
	public static boolean isNullOrEmpty(String str) {
		return (str == null || str.trim().isEmpty());
	}
	
	/**
	 * @function   判断一个数组是否为null或空值
	 * @param strs 待判断的数组
	 * @return
	 */
	public static boolean isNullOrEmpty(Object[] strs) {
		return (strs == null || 0 == strs.length);
	}
	
	/**
	 * @function  判断一个Set是否为null或空值
	 * @param set 待判断的Set
	 * @return
	 */
	public static boolean isNullOrEmpty(Set<?> set) {
		return (set == null || 0 == set.size());
	}

	/**
	 * @function   判断一个List是否为null或空值
	 * @param list 待判断的List
	 * @return
	 */
	public static boolean isNullOrEmpty(List<?> list) {
		return (list == null || 0 == list.size());
	}


	/**
	 * @function  判断一个Map是否为null或空值
	 * @param map 待判断的Map
	 * @return
	 */
	public static boolean isNullOrEmpty(Map<?, ?> map) {
		return (map == null || 0 == map.size());
	}
	

	/**
	 * @function  转换Set<String>到String数组
	 * @param set 待转换的集合
	 * @return    转换后的数组
	 */
	public static String[] toArray(Set<String> set) {
		if (isNullOrEmpty(set)) {
			return null;
		}
		int size = set.size(), idx = 0;
		String[] values = new String[size];
		for (String value : set) {
			values[idx++] = value;
		}
		return values;
	}
	
		
	/**
	 * @function  从配置文件中读取tag标签下的字符串集合
	 * @param tag 待查找的标签
	 * @return    从配置文件中读取生成的set (不会为NULL)
	 */
	public static HashSet<String> getSetFromTag(String tag) {
		HashSet<String> valueSet = new HashSet<String>();
		if (!isNullOrEmpty(tag)) {
			//获取tag对应的值，用分隔符对其进行分割
			String line = ConfigFactory.getString(tag);
			if (!isNullOrEmpty(line)) {
				String[] values = line.split(Common.SEPARATOR);
				if (!isNullOrEmpty(values)) {
					for (String value : values) {
						valueSet.add(value);
					}
				}
			}
		}
		return valueSet;
	}
	

	/**
	 * @function  从配置文件读取tag标签下的字段映射
	 * @param tag 待查找的标签
	 * @return    从配置文件中读取生成的map (不会为NULL)
	 */
	public static HashMap<String, String> getMapFromTag(String tag) {
		HashMap<String, String> map = new HashMap<String, String>();
		//先获取tag对应的值的字符串集合
		Set<String> valueSet = getSetFromTag(tag);
		if (!isNullOrEmpty(valueSet)) {
			for (String value : valueSet) {
				if (CommonUtil.isNullOrEmpty(value)) {
					continue;
				}
				String[] valueArray = value.split(Common.MAPSEPARATOR);
				if (valueArray != null && valueArray.length > 1) {
					map.put(valueArray[0], valueArray[1]);
				}
			}
		}
		return map;
	}
	
	
	/**
	 * @function  从字符串中获取字符串集合（以separator分割）
	 * @param str 待分割的字符串
	 * @param separator 分隔符号
	 * @return    分割后的字符串集合
	 */
	public static HashSet<String> getSetFromString(String str, String separator) {
		if (isNullOrEmpty(str) || isNullOrEmpty(separator)) {
			return null;
		}
		String[] values = str.split(separator);
		if (!isNullOrEmpty(values)) {
			HashSet<String> set = new HashSet<String>();
			for (String value : values) {
				set.add(value);
			}
			return set;
		}
		return null;
	}
	
	
	/**
	 * @function  从字符串中获取整数集合（以separator分割）
	 * @param str 待分割的字符串集合
	 * @param separator	分隔符
	 * @return    被分隔符分割后的整数集合
	 */
	public static HashSet<Integer> getIntSetFromString(String str, String separator) {
		HashSet<String> set = getSetFromString(str, separator);
		if (null == set) {
			return null;
		}
		HashSet<Integer> intSet = new HashSet<Integer>();
		for (String value : set) {
			if (value.matches(Common.REGEXNUMBER)) {
				intSet.add(Integer.valueOf(value));
			}
		}
		return intSet;
	}
	
	/**
	 * @function 将数组转换成集合
	 * @param    array 待转换的数组
	 * @return   转换后的集合
	 */
	public static Set<String> getSetFromArray(String[] array) {
		Set<String> set = new HashSet<String>();
		if (array != null) {
			for (String item : array) {
				set.add(item);
			}
		}
		return set;
	}
	
	
	/**
	 * @function 获取本工程所在的路径
	 * @return   本工程所在的根目录在系统中的绝对路径
	 */
	public static String getDefaultConfPath() {
		String defaultConfigPath = ConfigFactory.getString(Common.CONFIGPATH, "");
		if (defaultConfigPath.isEmpty()) {
			LOG.error("get default root path error!");
		}
		return defaultConfigPath;
	}
	
	/**
	 * @function 判断job是否是全量非索引job
	 * @param 	 jobName 待判断的job名称
	 * @return   判断结果
	 */
	public static boolean isBigUnIndexJob(String jobName) {
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName is an unavailable parament in CommonUtil:isBigIndexJob");
			return false;
		}
		jobName = jobName.toLowerCase();
		return (jobName.contains(Common.BIG) && !jobName.contains(Common.INDEX));
	}

	/**
	 * @function 判断job是否是增量非索引job
	 * @param 	 jobName 待判断的job名称
	 * @return   判断结果
	 */
	public static boolean isDyUnIndexJob(String jobName) {
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName is an unavailable parament in CommonUtil:isBigIndexJob");
			return false;
		}
		jobName = jobName.toLowerCase();
		return (jobName.contains(Common.DY) && !jobName.contains(Common.INDEX));
	}
	
	/**
	 * @function 判断job是否是全量索引job
	 * @param 	 jobName 待判断的job名称
	 * @return   判断结果
	 */
	public static boolean isBigIndexJob(String jobName) {
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName is an unavailable parament in CommonUtil:isBigIndexJob");
			return false;
		}
		jobName = jobName.toLowerCase();
		return (jobName.contains(Common.BIG) && jobName.contains(Common.INDEX));
	}
	
	/**
	 * @function 判断job是否是增量索引job
	 * @param 	 jobName 待判断的job名称
	 * @return   判断结果
	 */
	public static boolean isDyIndexJob(String jobName) {
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName is an unavailable parament in CommonUtil:isBigIndexJob");
			return false;
		}
		jobName = jobName.toLowerCase();
		return (jobName.contains(Common.DY) && jobName.contains(Common.INDEX));
	}
	
	/**
	 * @function 判断job是否是索引job
	 * @param 	 jobName 待判断的job名称
	 * @return   判断结果
	 */
	public static boolean isIndexJob(String jobName) {
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName is an unavailable parament in CommonUtil:isBigIndexJob");
			return false;
		}
		return (jobName.toLowerCase().contains(Common.INDEX));
	}
	
	
	/**
	 * @function 判断job是否是全量job
	 * @param 	 jobName 待判断的job名称
	 * @return   判断结果
	 */
	public static boolean isBigJob(String jobName) {
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName is an unavailable parament in CommonUtil:isBigIndexJob");
			return false;
		}
		return (jobName.toLowerCase().contains(Common.BIG));
	}
	
	/**
	 * @function 判断job是否增量job
	 * @param 	 jobName 待判断的job名称
	 * @return   判断结果
	 */
	public static boolean isDyJob(String jobName) {
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName is an unavailable parament in CommonUtil:isBigIndexJob");
			return false;
		}
		return (jobName.toLowerCase().contains(Common.DY));
	}
	
}
