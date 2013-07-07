/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.conf;

import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;
import com.panguso.datatransfer.util.CommonUtil;


/**
 * @author zhaopeng
 *
 */
public class FieldConf {
	private static final Logger LOG = LoggerFactory.getLogger(FieldConf.class);
	private String tablePrefix;
	
	private  String[] fields = null;
	private  String[] longfields = null;
	private  String[] intfields = null;
	private  String[] floatfields = null;
	private  String[] datetimefields = null;
	private  String[] timefields = null;
	private  String[] timestampfields = null;
	private  String[] doublefields = null;
	private  String[] clobfields = null;
	public String[] getFields() {
		return fields;
	}


	private  HashSet<String> longset;
	private  HashSet<String> intset;
	private  HashSet<String> floatset;
	private  HashSet<String> datetimeset;
	private  HashSet<String> timeset;
	private  HashSet<String> timestampset;
	private  HashSet<String> doubleset;
	private  HashSet<String> clobset;
	
	public HashSet<String> getLongset() {
		return longset;
	}


	public HashSet<String> getIntset() {
		return intset;
	}


	public HashSet<String> getFloatset() {
		return floatset;
	}


	public HashSet<String> getDatetimeset() {
		return datetimeset;
	}


	public HashSet<String> getTimeset() {
		return timeset;
	}


	public HashSet<String> getTimestampset() {
		return timestampset;
	}


	public HashSet<String> getDoubleset() {
		return doubleset;
	}


	public HashSet<String> getClobset() {
		return clobset;
	}


	public void setFields(String[] fields) {
		this.fields = fields;
	}
	
	/**
	 * 构造函数
	 * @param prefix 表名
	 */
	public FieldConf(String prefix) {
		this.tablePrefix = prefix;
	}

	/**
	 * 初始化方法
	 */
	public void init() {
		longset = new HashSet<String>();
		intset = new HashSet<String>();
		floatset = new HashSet<String>();
		datetimeset = new HashSet<String>();
		timeset = new HashSet<String>();
		timestampset = new HashSet<String>();
		doubleset = new HashSet<String>();
		clobset = new HashSet<String>();
		initFields();
	}
	
	
	/**
	 * 设置字段
	 */
	public void setFields() {
		String fieldStr = ConfigFactory.getString(tablePrefix + Common.FIELD);
		if (CommonUtil.isNullOrEmpty(fieldStr)) {
			LOG.error("初始化表信息失败：字段为空！");
			fields = new String[0];
		} else {
			String[] segs = fieldStr.split(Common.SEPARATOR);
			fields = new String[segs.length];
			for (int i = 0; i < segs.length; i++) {
				fields[i] = segs[i].trim().toLowerCase();
			}
		}
	}
	
	
	/**
	 * 设置长整型字段
	 */
	public void setLongFields() {
		String path = tablePrefix + "longfields";
		String s = ConfigFactory.getString(path);
		if (null == s) {
			longfields = new String[0];
		} else {
			String[] field = s.split(";");
			longfields = new String[field.length];
			for (int i = 0; i < field.length; i++) {
				longfields[i] = field[i].trim().toLowerCase();
			}
		}	
	}
	
	/**
	 * 设置布尔型字段
	 */
	public void setBlobFields() {
		String path = tablePrefix + "blobfields";
		String s = ConfigFactory.getString(path);
		if (null == s) {
			clobfields = new String[0];
		} else {
			String[] field = s.split(";");
			clobfields = new String[field.length];
			for (int i = 0; i < field.length; i++) {
				clobfields[i] = field[i].trim().toLowerCase();
			}
		}	
	}
	
	/**
	 * 设置整形字段
	 */
	public void setIntFields() {
		String path = tablePrefix + ".intfields";
		String s = ConfigFactory.getString(path);
		if (null == s) {
			intfields = new String[0];
		} else {
			String[] field = s.split(";");
			intfields = new String[field.length];
			for (int i = 0; i < field.length; i++) {
				intfields[i] = field[i].trim().toLowerCase();
			}
		}	
	}
	
	/**
	 * 设置浮点型字段
	 */
	public void setFloatFields() {
		String path = tablePrefix + ".floatfields";
		String s = ConfigFactory.getString(path);
		if (null == s) {
			floatfields = new String[0];
		} else {
			String[] field = s.split(";");
			floatfields = new String[field.length];
			for (int i = 0; i < field.length; i++) {
				floatfields[i] = field[i].trim().toLowerCase();
			}
		}	
	}
	
	/**
	 * 设置时间型字段
	 */
	public void setDatetimeFields() {
		String path = tablePrefix + ".datetimefields";
		String s = ConfigFactory.getString(path);
		if (null == s) {
			datetimefields = new String[0];
		} else {
			String[] field = s.split(";");
			datetimefields = new String[field.length];
			for (int i = 0; i < field.length; i++) {
				datetimefields[i] = field[i].trim().toLowerCase();
			}
		}
	}
	
	/**
	 * 设置时间型字段
	 */
	public void setTimeFields() {
		String path = tablePrefix + ".timefields";
		String s = ConfigFactory.getString(path);
		if (null == s) {
			timefields = new String[0];
		} else {
			String[] field = s.split(";");
			timefields = new String[field.length];
			for (int i = 0; i < field.length; i++) {
				timefields[i] = field[i].trim().toLowerCase();
			}
		}
	}
	
	/**
	 * 设置时间字段
	 */
	public void setTimeStampFields() {
		String path = tablePrefix + ".timestampfields";
		String s = ConfigFactory.getString(path);
		if (null == s) {
			timestampfields = new String[0];
		} else {
			String[] field = s.split(";");
			timestampfields = new String[field.length];
			for (int i = 0; i < field.length; i++) {
				timestampfields[i] = field[i].trim().toLowerCase();
			}
		}
	}
	
	/**
	 * 设置长浮点型字段
	 */
	public void setDoubleFields() {
		String path = tablePrefix + ".doublefields";
		String s = ConfigFactory.getString(path);
		if (null == s) {
			doublefields = new String[0];
		} else {
			String[] field = s.split(";");
			doublefields = new String[field.length];
			for (int i = 0; i < field.length; i++) {
				doublefields[i] = field[i].trim().toLowerCase();
			}
		}
	}
	
	
	/**
	 * 初始化字段
	 */
	public void initFields() {
		setFields();

		setLongFields();
		setIntFields();
		setFloatFields();
		setDatetimeFields();
		setTimeFields();
		setTimeStampFields();
		setDoubleFields();
		setBlobFields();
		
		for (String fe: longfields) {
			longset.add(fe.trim().toLowerCase());
		}
		for (String fe: intfields) {
			intset.add(fe.trim().toLowerCase());
		}
		for (String fe: floatfields) {
			floatset.add(fe.trim().toLowerCase());
		}
		for (String fe: datetimefields) {
			datetimeset.add(fe.trim().toLowerCase());
		}
		for (String fe: timefields) {
			timeset.add(fe.trim().toLowerCase());
		}
		for (String fe: timestampfields) {
			timestampset.add(fe.trim().toLowerCase());
		}
		for (String fe: doublefields) {
			doubleset.add(fe.trim().toLowerCase());
		}
		for (String fe: clobfields) {
			clobset.add(fe.trim().toLowerCase());
		}
	}
	
}
