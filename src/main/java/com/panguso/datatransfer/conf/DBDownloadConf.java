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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;
import com.panguso.datatransfer.util.CommonUtil;

/**
 * @author liubing
 *
 */
public class DBDownloadConf {
	private static final Logger LOG = LoggerFactory.getLogger(DBDownloadConf.class);	
	private String prefix = null;
	private List<String> fromTableList = null;
	private String toTable = null;
//	private Map<String, HashSet<Integer>> tableSkip = null;
	private Map<String, HashMap<String, String>> tableMap = null;
	
	public List<String> getFromTableList() {
		return fromTableList;
	}

	public void setFromTableList(List<String> fromTableList) {
		this.fromTableList = fromTableList;
	}

	public String getToTable() {
		return toTable;
	}

//	public Map<String, HashSet<Integer>> getTableSkip() {
//		return tableSkip;
//	}

	public Map<String, HashMap<String, String>> getTableMap() {
		return tableMap;
	}
	

	/**
	 * 构造函数
	 * @param prefix 与本下载配置相关的XPath前缀
	 */
	public DBDownloadConf(String prefix) {
		this.prefix = prefix + Common.DBPREFIX;
		fromTableList = new ArrayList<String>();
	}
	
	/**
	 * 初始化方法
	 */
	public void init() {
		LOG.info("Begin to init DbDownloadConf ...");
		//初始化DB下载参数 数据来源表
		String fromTableStr = ConfigFactory.getString(prefix + Common.FROMTABLE);
		if (CommonUtil.isNullOrEmpty(fromTableStr)) {
			LOG.error("ERROR:init DbDownloadConf error, no config tables for from_table tag!");
			System.exit(-1);
		}
		
		String[] fromTables = fromTableStr.split(Common.SEPARATOR);
		if (null == fromTableList) {
			fromTableList = new ArrayList<String>();
		}
		for (String table : fromTables) {
			fromTableList.add(table);
		}
		//初始化来源表的忽略字段、映射字段
		initTableMap(fromTableList);
//		initTableSkip();
		
		//初始化DB下载参数 写入表
		String totable = ConfigFactory.getString(prefix + Common.TOTABLE);
		if (CommonUtil.isNullOrEmpty(totable)) {
			LOG.error("ERROR:init DbDownloadConf error, no tables configed for to_table tag!");
			System.exit(-1);
		}
		this.toTable = totable;
				
		LOG.info("init DbDownloadConf success");
	}
	
	
//	/**
//	 * 初始化数据表接入时要忽略的字段
//	 */
//	private void initTableSkip() {
//		if (null == tableSkip) {
//			tableSkip = new HashMap<String, HashSet<Integer>>(); 
//		}
//		
//		String fileSkipMapTag = prefix + Common.FILESKIP;
//		HashMap<String, String> skipMap = CommonUtil.getMapFromTag(fileSkipMapTag);
//		if (skipMap != null) {
//			for (String file : skipMap.keySet()) {
//				String fileSkipStr = skipMap.get(file);
//				HashSet<Integer> intSet = CommonUtil.getIntSetFromString(fileSkipStr, Common.COMMA);
//				if (!CommonUtil.isNullOrEmpty(intSet)) {
//					tableSkip.put(file, intSet);
//				}
//			}
//		} else {
//			LOG.error("init initFieldFieldSkip failed! please check file_skip tag");
//			System.exit(-1);
//		}
//		
//		
//		/*
//		if (CommonUtil.isNullOrEmpty(tableList)) {
//			LOG.error("init from_table tag error, There are no tables configed in from_table!");
//			return;
//		}
//		
//		if (null == tableSkip) {
//			tableSkip = new HashMap<String, HashSet<Integer>>();
//		}
//		
//		if (prefix != null) {
//			for (String table : tableList) {				
//				String fieldSkipTag = prefix + Common.DOT + table + Common.SKIP;
//				Set<String> set = CommonUtil.getSetFromTag(fieldSkipTag);
//				HashSet<Integer> skipSet = new HashSet<Integer>();
//				if (set != null) {
//					for (String value : set) {
//						if (value.matches(Common.REGEXNUMBER)) {
//							skipSet.add(Integer.valueOf(value));
//						}
//					}
//				} 
//				tableSkip.put(table, skipSet);
//			}
//		} else {
//			LOG.warn("init DbDownloadConf error, prefix is null or empty!");
//		}
//		*/		
//	}
		

	/**
	 * 初始化数据表字段映射关系
	 * @param tableList 来源表的
	 */
	private void initTableMap(List<String> tableList) {
		if (CommonUtil.isNullOrEmpty(tableList)) {
			LOG.warn("init db tag of job error：from_table is null or empty!");
			return;
		}
		
		if (null == tableMap) {
			tableMap = new HashMap<String, HashMap<String, String>>();
		}
		for (String table : tableList) {				
			String fieldMapTag = prefix + Common.DOT + table + Common.MAP;
			HashMap<String, String> map = CommonUtil.getMapFromTag(fieldMapTag);
			if (null == map) {
				map = new HashMap<String, String>();
			}
			tableMap.put(table, map);
		}
	}
}
