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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;
import com.panguso.datatransfer.util.CommonUtil;

/**
 * @author liubing
 *
 */
public class IndexConf {
	private static final Logger LOG = LoggerFactory.getLogger(IndexConf.class);
	
	private String indexName = null;
	private String indexCmd = null;
	//索引建立之前，要等待一些job接入完成
	private boolean waitForJobEnable = false;
	private Set<String> waitForJobSet = null;
	//全量索引建立之前，要将增量表数据导入全量表，建立完成以后，删除增量表中的数据
	private Set<String> involveTableSet = null;

	private String prefix = null;
	
	public boolean getWaitForJobEnable() {
		return waitForJobEnable;
	}
	
	public Set<String> getWaitForJobSet() {
		return waitForJobSet;
	}
	
	public Set<String> getInvolveTableSet() {
		return involveTableSet;
	}
	
	public String getIndexCmd() {
		return indexCmd;
	}
	/**
	 * @param prefix XPath路径
	 * @param indexName 索引job的名称
	 */
	public IndexConf(String prefix, String indexName) {
		this.indexName = indexName;
		this.prefix = prefix;
		waitForJobSet = new HashSet<String>();
		involveTableSet = new HashSet<String>();
	}
	
	/**
	 * 初始化函数
	 */
	public void init() {
		if (CommonUtil.isNullOrEmpty(prefix)) {
			LOG.error("init IndexConf error! prefix is null or empty!");
			System.exit(-1);
		}
		//获取建立索引时是否需要等待接入job完成的job列表
		String waitForJobEnableTag = prefix + Common.WAITFORJOBENABLE;
		waitForJobEnable = ConfigFactory.getBoolean(waitForJobEnableTag);
		
		//获取索引需要等待的job列表
		if (waitForJobEnable) {
			String waitForJobTag = prefix + Common.WAITFORJOBS;
			waitForJobSet = CommonUtil.getSetFromTag(waitForJobTag);
			//验证该job列表
			if (null == waitForJobSet) {
				LOG.error("init IndexConf for {} error, check wait_for_jobs tag!", indexName);
				System.exit(-1);
			}
			LOG.info("init waitForJobSet for job {} success, need wait for {}", indexName, waitForJobSet.toString());
		}
		//获取全量索引需要转移、融合的数据表
		if (CommonUtil.isBigIndexJob(indexName)) {
			String involveTableTag = prefix + Common.INVOLVETABLES;
			involveTableSet = CommonUtil.getSetFromTag(involveTableTag);
			if (CommonUtil.isNullOrEmpty(involveTableSet)) {
				LOG.warn("init IndexConf for {} warning, config wait for involve_tables is empty!", indexName);
				//System.exit(-1);
			}
			LOG.info("init involveTableSet for job {} success, involve tables: {}", indexName, involveTableSet.toString());
		}
		
		indexCmd = ConfigFactory.getString(prefix + Common.INDEXCMD, "");
	}
}
