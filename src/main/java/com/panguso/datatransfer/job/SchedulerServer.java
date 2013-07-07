/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.job;

import com.panguso.datatransfer.conf.JobConf;
import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.GlobalVariable;
import com.panguso.datatransfer.util.CommonUtil;
import com.panguso.datatransfer.util.DateUtil;

import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liubing
 *
 */
public final class SchedulerServer {
	private static final Logger LOG = LoggerFactory.getLogger(SchedulerServer.class);
	private static Scheduler scheduler;
//	private static HashMap<String, JobConf> bigJobConfMap = null;
//	private static HashMap<String, JobConf> dyJobConfMap = null;
	private static HashMap<String, JobConf> jobConfMap = null;
	private SchedulerServer() {
		
	}
	
	//初始化各参数变量
	private static void init() {
		try {
			scheduler = StdSchedulerFactory.getDefaultScheduler();
		} catch (SchedulerException e) {
			LOG.error("get job Scheduler error in SchedulerServer:init()!");
			System.exit(-1);
		}
		
		//获取所有的jobConf的配置
		jobConfMap = GlobalVariable.getJobConfMap();
		if (null == jobConfMap) {
			LOG.error("get jobConfMap error in SchedulerServer:init()!");
			System.exit(-1);
		}
		/*
		//获取全量job
		bigJobConfMap = GlobalVariable.getBigjobConfMap();
		if (null == bigJobConfMap) {
			LOG.error("get bigJobConfMap error in SchedulerServer:initVariables()!");
			System.exit(-1);
		}
		//获取增量job
		dyJobConfMap = GlobalVariable.getDyjobConfMap();
		if (null == dyJobConfMap) {
			LOG.error("get dyJobConfMap error in SchedulerServer:initVariables()!");
			System.exit(-1);
		}
		*/
	}
	
	
	/**
	 * 获取jobName的配置信息jobConf
	 * @param jobName job名称
	 * @return 该job对应的JobConf配置信息
	 */
	private static JobConf getJobConf(String jobName) {
		// 检查参数job名称的可用性
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName is null or empty, getJobConf error in SchedulerServer:getJobConf()!");
			System.exit(-1);
		}
		// 检查jobConfMap参数的可用性
		if (null == jobConfMap) {
			LOG.error("jobConfMap is null, getJobConf error in SchedulerServer:getJobConf()!");
			System.exit(-1);			
		}
		
		return jobConfMap.get(jobName);
		/*
		if (jobName.startsWith(Common.DY) && dyJobConfMap != null) {
			return dyJobConfMap.get(jobName);
		} else if (jobName.startsWith(Common.BIG) && bigJobConfMap != null) {
			return bigJobConfMap.get(jobName);
		}
		return null;
		*/
	}
	

	/**
	 * 启动定时调度任务
	 * @param jobNames 待调度的Job名称列表
	 * @throws SchedulerException 调度异常
	 * @throws ParseException 解析异常
	 */
	public static void startScheduler(String[] jobNames) throws SchedulerException,
			ParseException {
		
		//初始化
		init();
		
		//如果参数无效，则使用全局job参数变量
		if (CommonUtil.isNullOrEmpty(jobNames)) {
//			Set<String> jobSet = new HashSet<String>();
			
			//检查参数jobConfMap的可用性
			if (CommonUtil.isNullOrEmpty(jobConfMap)) {
				LOG.warn("No jobs in this schedule process!");
				return;	
			} else {
				jobNames = CommonUtil.toArray(jobConfMap.keySet());
			}
			
//			if (bigJobConfMap != null || dyJobConfMap != null) {
//				jobSet.addAll(bigJobConfMap.keySet());
//				jobSet.addAll(dyJobConfMap.keySet());
//				jobNames = CommonUtil.toArray(jobSet);
//			} else {
//				LOG.warn("There are no jobs in this schedule process!");
//				return;
//			}
			
			if (null == jobNames) {
				LOG.warn("no jobs need to launched!");
				return;
			}
		}
		
		//为每个job创建一个lock file,用于互斥操作
	//	FileUtil.createLockFile(jobNames);
		
		//为每一个job执行调度操作准备 
		for (String jobName : jobNames) {
			JobConf jobConf = getJobConf(jobName);
			if (null == jobConf) {
				continue;
			}
			String crontab = jobConf.getCrontab();
			LOG.info("begin to launch " + jobName + ", Class: " + jobConf.getJobClass() + ", Crontab: " + crontab);
			
			//获取job的执行CLASS
			JobDetail job = null;
			try {
				job = new JobDetail("job", jobName, Class.forName(jobConf.getJobClass()));
			} catch (ClassNotFoundException e) {
				LOG.error("launch job {} failed，{}", jobName, e);
				continue;
			}
			//获取job的调度周期crontab
			CronTrigger trigger = new CronTrigger("trigger", jobName, "job", jobName, crontab);

			scheduler.scheduleJob(job, trigger);			
		}
		//开始执行所有job的调度
		scheduler.start();
	}

	
	/**
	 * 启动定时调度任务
	 * @throws SchedulerException 调度异常
	 * @throws ParseException 解析异常
	 */
	public static void schedulerBigjobs() throws SchedulerException,
			ParseException {
		
		//初始化
		init();
		
		String[] bigJobNames = null;
		if (CommonUtil.isNullOrEmpty(jobConfMap)) {
			LOG.warn("No jobs in this schedule process!");
			return;	
		} else {
			//获取所有bigjobs
			Set<String> bigJobSet = new HashSet<String>();
			for (String name : jobConfMap.keySet()) {
				if (name.startsWith(Common.BIG)) {
					bigJobSet.add(name);
				}
			}
			bigJobNames = CommonUtil.toArray(bigJobSet);
		}
				
		if (null == bigJobNames) {
			LOG.warn("no big jobs need to launched!");
			return;
		}
		
		//获取job的调度周期crontab，即当前时间的一分钟之后
		String crontab = DateUtil.getCrotabOfNextMinute();
		
		//为每一个job执行调度操作准备 
		for (String jobName : bigJobNames) {
			JobConf jobConf = getJobConf(jobName);
			if (null == jobConf) {
				continue;
			}
			LOG.info("begin to launch {},  Crontab: {}", jobName, crontab);
			
			//获取job的执行CLASS
			JobDetail job = null;
			try {
				job = new JobDetail("job", jobName, Class.forName(jobConf.getJobClass()));
			} catch (ClassNotFoundException e) {
				LOG.error("launch job {} failed，{}", jobName, e);
				continue;
			}
			//开始调度
			CronTrigger trigger = new CronTrigger("trigger", jobName, "job", jobName, crontab);
			scheduler.scheduleJob(job, trigger);			
		}
		//开始执行所有job的调度
		scheduler.start();
	}
	
	
	/**
	 * 关闭定时调度任务
	 * @throws Exception 调度异常
	 */
	public static void shutdown() throws Exception {
		if (scheduler != null) {
			scheduler.shutdown();
		}
	}

}