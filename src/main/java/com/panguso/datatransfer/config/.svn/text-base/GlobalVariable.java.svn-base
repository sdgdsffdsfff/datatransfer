/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.conf.JobConf;
import com.panguso.datatransfer.conf.TableConf;
import com.panguso.datatransfer.util.CommonUtil;

/**
 * @author liubing
 *
 */
public final class GlobalVariable {
	private static final Logger LOG = LoggerFactory.getLogger(GlobalVariable.class);
	
	private static HashMap<String, JobConf> bigjobConfMap = null;
	private static HashMap<String, JobConf> dyjobConfMap = null;
	private static HashMap<String, JobConf> jobConfMap = null;
	private static HashMap<String, TableConf> tableConfMap = null;
	
	/**
	 * 构造函数 
	 */
	private GlobalVariable() {
	}
	
	public static HashMap<String, JobConf> getBigjobConfMap() {
		return bigjobConfMap;
	}


	public static HashMap<String, JobConf> getDyjobConfMap() {
		return dyjobConfMap;
	}

	public static HashMap<String, JobConf> getJobConfMap() {
		return jobConfMap;
	}
	
	public static HashMap<String, TableConf> getTableConfMap() {
		return tableConfMap;
	}

	/**
	 * 根据传递的job类型字符串参数返回job配置map
	 * @param jobPrefix job前缀
	 * @return job名称与job配置文件信息的map
	 */
	public static Map<String, JobConf> getJobConfMap(String jobPrefix) {
		LOG.debug("jobType: " + jobPrefix);
		if (CommonUtil.isNullOrEmpty(jobPrefix)) {
			LOG.warn("get jobConfMap failed，please check config.xml about dyjobs and bigjobs tag！");
			return null;
		}
		return jobConfMap;		
		
//		if (jobPrefix.startsWith("big")) {
//			return bigjobConfMap;
//		} else if (jobPrefix.startsWith("dy")) {
//			return dyjobConfMap;
//		}
//		return null;
	}

	/**
	 * 根据传递的job名称参数返回其对应的JobConf
	 * @param jobPrefix job名称
	 * @return
	 */
	public static JobConf getJobConf(String jobPrefix) {
		Map<String, JobConf> map = getJobConfMap(jobPrefix);
		if (null == map) {
			return null;
		}
		return map.get(jobPrefix);
	}
	
	/**
	 * 初始化各全局变量
	 * @return
	 */
	public static void init() {
		bigjobConfMap = new HashMap<String, JobConf>();
		dyjobConfMap = new HashMap<String, JobConf>();
		jobConfMap = new HashMap<String, JobConf>();
		tableConfMap = new HashMap<String, TableConf>();

		//初始化全量job
		String prefix = Common.BIGJOBS;
		initBigJobInfo(prefix);

		//初始化增量job
		prefix = Common.DYJOBS;
		initDyJobInfo(prefix);
		
		//初始化索引job
		prefix = Common.INDEXJOBS;
		initindexJobInfo(prefix);
		
		
		//获取Table列表
		prefix = Common.TABLES;
		initTableInfo(prefix);
		
		LOG.info("init Global Variables success!");
	}
	

	/**
	 * 初始化增量job列表
	 * @param prefix dyJob的XPath路径
	 */
	private static void initDyJobInfo(String prefix) {
		LOG.info("Begin to init dyjobs......");
		
		int jobNum = ConfigFactory.getInt(prefix + Common.JOBNUMBER, 0);	
		if (0 == jobNum) {
			LOG.warn("There are no dyjobs in config.xml!");
			return;
		}
		
		String base = prefix + Common.DYJOB + "(0)";
		if (null == dyjobConfMap) {
			dyjobConfMap = new HashMap<String, JobConf>();
		}

		if (null == jobConfMap) {
			jobConfMap = new HashMap<String, JobConf>();
		}
		
		for (int i = 0; i < jobNum; i++) {
			String path = base.replaceAll("0", String.valueOf(i));
			//获取Job名称，初始化job相关配置信息
			String jobID = ConfigFactory.getString(path + Common.ATTRIBUTE);
			JobConf jobConf = new JobConf(path);
			jobConf.init();			
			jobConfMap.put(jobID, jobConf);
			dyjobConfMap.put(jobID, jobConf);
		}
		
		LOG.info("init dy job success, and There are total {} dyjobs", jobNum);
	}
	
	
	/**
	 * 初始化全量job列表 
	 * @param prefix 全量数据接入job的XPath路径
	 */
	private static void initBigJobInfo(String prefix) {
		LOG.info("Begin to init bigjobs......");
		
		int jobNum = ConfigFactory.getInt(prefix + Common.JOBNUMBER, 0);	
		if (0 == jobNum) {
			LOG.info("There are no bigjobs in this project!");
			return;
		}
		
		String base = prefix + Common.BIGJOB + "(0)";
		if (null == bigjobConfMap) {
			bigjobConfMap = new HashMap<String, JobConf>();
		}

		if (null == jobConfMap) {
			jobConfMap = new HashMap<String, JobConf>();
		}
		
		for (int i = 0; i < jobNum; i++) {
			String path = base.replaceAll("0", String.valueOf(i));
			String jobID = ConfigFactory.getString(path + Common.ATTRIBUTE);
			JobConf jobConf = new JobConf(path);
			jobConf.init();
			jobConfMap.put(jobID, jobConf);
			bigjobConfMap.put(jobID, jobConf);
		}
		
		LOG.info("init big job success, and There are total {} bigjobs", jobNum);
	}
			
	/**
	 * 初始化索引job列表 
	 * @param prefix 索引job的XPath路径
	 */
	private static void initindexJobInfo(String prefix) {
		LOG.info("Begin to init index jobs......");
		
		int jobNum = ConfigFactory.getInt(prefix + Common.JOBNUMBER, 0);	
		if (0 == jobNum) {
			LOG.info("WARN: There are no indexjobs in this project!");
			return;
		}
		
		if (null == jobConfMap) {
			jobConfMap = new HashMap<String, JobConf>();
		}
		
		String base = prefix + Common.INDEXJOB + "(0)";
		for (int i = 0; i < jobNum; i++) {
			String path = base.replaceAll("0", String.valueOf(i));
			String jobID = ConfigFactory.getString(path + Common.ATTRIBUTE);
			JobConf jobConf = new JobConf(path);
			jobConf.init();
			jobConfMap.put(jobID, jobConf);
			//增量index放入dyjobConfMap 全量index放入bigjobConfMap
			if (jobID.contains(Common.BIG)) {
				bigjobConfMap.put(jobID, jobConf);
			} else if (jobID.contains(Common.DY)) {
				dyjobConfMap.put(jobID, jobConf);
			}
		}
		
		LOG.info("init index jobs success, and There are total {} indexjobs", jobNum);
	}
	
	/**
	 * 初始化数据库的表信息
	 * @param prefix XPath路径
	 */
	private static void initTableInfo(String prefix) {
		LOG.info("Begin to init tables");
		
		//获取表个数，无论如何表的个数都应该大于0
		int tableNum = ConfigFactory.getInt(prefix + Common.TABLENUM, 0);	
		if (tableNum == 0) {
			LOG.error("init table info error, There are no tables config in this project!");
			System.exit(-1);
		}
		
		String base = prefix + Common.TABLE + "(0)";
		if (null == tableConfMap) {
			tableConfMap = new HashMap<String, TableConf>();
		}
		for (int i = 0; i < tableNum; i++) {
			String path = base.replaceAll("0", String.valueOf(i));
			String tableID = ConfigFactory.getString(path + Common.ATTRIBUTE);
			TableConf tableConf = new TableConf(path);
			tableConf.init();
			tableConfMap.put(tableID, tableConf);
		}
		
		LOG.info("init tables success, and There are total " + tableConfMap.size() 
				+ " tables: " + tableConfMap.keySet().toString());
	}
	
	/**
	 * 检查from table 列表是否全都可用，即是否都在tables配置文件中
	 * @param tableList 待检查的table列表
	 * @return 检查结果
	 */
	public static boolean isAllTablesAvailable(List<String> tableList) {
		if (CommonUtil.isNullOrEmpty(tableList)) {
			LOG.error("from table list is null or empty, please check!");
			return false;
		}
		//逐个对from table进行检查
		boolean allAvailable = true;		
		for (String table : tableList) {
			if (!isTableAvailable(table)) {
				LOG.error("{} is not available! please check from_table and tables tag", table);
				allAvailable = false;
				break;
			}
		}
		return allAvailable;
	}
	
	/**
	 * @function    检查table是否全都可用，即都在tables配置文件中
	 * @param table 待检查的table表名
	 * @return      检查结果
	 */
	public static boolean isTableAvailable(String table) {
		if (CommonUtil.isNullOrEmpty(table)) {
			LOG.error("table name is null or empty, please check!");
			return false;
		}
		return tableConfMap.containsKey(table);
	}
	
	/**
	 * 返回本项目中所有的index job
	 * @return index job 列表
	 */
	public static List<String> getAllIndexJob() {
		List<String> allIndexJobList = new ArrayList<String>();
		if (CommonUtil.isNullOrEmpty(jobConfMap)) {
			LOG.error("jobConfMap is null or empty， cannot get index job!");
			return allIndexJobList;
		}
		
		//遍历所有job，集合所有index job
		for (String job : jobConfMap.keySet()) {
			if (job.contains(Common.INDEX)) {
				allIndexJobList.add(job);
			}
		}
		
		return allIndexJobList;
	}
	
//	public static void main(String[] argv) {
//		Map<String, String> map = new HashMap<String, String>();
//		map.put("key_one", "value_one");
//		map.put("key_two", "value_one");
//		map.put("key_three", "value_one");
//		
//		LOG.info(map.containsKey("key_one") + "");
//		LOG.info(map.containsKey("key_four") + "");
//	}
	
}
