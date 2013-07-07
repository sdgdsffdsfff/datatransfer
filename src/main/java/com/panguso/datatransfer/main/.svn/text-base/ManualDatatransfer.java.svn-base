/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;
import com.panguso.datatransfer.config.GlobalVariable;
import com.panguso.datatransfer.config.LogFactory;
import com.panguso.datatransfer.job.SchedulerServer;
import com.panguso.datatransfer.util.CommonUtil;
import com.panguso.datatransfer.util.SQLUtil;

/**
 * @author liubing
 *
 */
public class ManualDatatransfer {
	private static final Logger LOG = LoggerFactory.getLogger(ManualDatatransfer.class);
	/**
	 * 构造函数 
	 */
	public ManualDatatransfer() {
		
	}
	
	/**
	 * 初始化方法
	 */
	public void init() {
		try {
			String confpath = System.getProperty("datatransfer");
			if (CommonUtil.isNullOrEmpty(confpath)) {
				LOG.error("get config path error! ");
				LOG.error("usage: java -Xmx1024m -Ddatatransfer=${ROOT_PATH}......");
				System.exit(-1);
			}
			LOG.debug("confpath: " + confpath);
			System.setProperty("datatransfer", confpath);
			
			LogFactory.config("./conf/logback.xml");
			ConfigFactory.init("./conf/config.xml");
			Common.initLastStamp();
			LOG.info("init logback.xml and config.xml success!");
			
			//初始化数据库连接信息
			SQLUtil.initDS();
			
			//初始化全部配置变量
			GlobalVariable.init();
		} catch (Exception e) {
			LOG.error("StandardDatatransfer init error {}", e);
			System.exit(-1);
		}
		LOG.info("StandardDatatransfer init success!");
	}
	
	
	/**
	 * @param args 
	 */
	public static void main(String[] args) {
		ManualDatatransfer dataTransfer = new ManualDatatransfer();
		try {
			dataTransfer.init();
			//调度JOB，job参数以全局变量提供
			SchedulerServer.startScheduler(null);
			
		} catch (Exception e) {
			LOG.error("Error when launch project！", e);
		}
	}
}
