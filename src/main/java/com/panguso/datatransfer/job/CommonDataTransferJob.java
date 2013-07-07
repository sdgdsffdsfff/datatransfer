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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.conf.IndexConf;
import com.panguso.datatransfer.conf.JobConf;
import com.panguso.datatransfer.conf.TableConf;
import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.GlobalVariable;
import com.panguso.datatransfer.service.CommonDownloadService;
import com.panguso.datatransfer.service.CommonImportService;
import com.panguso.datatransfer.service.CommonIndexService;
import com.panguso.datatransfer.service.CommonService;
import com.panguso.datatransfer.util.CommonUtil;
import com.panguso.datatransfer.util.DateUtil;
import com.panguso.datatransfer.util.FileUtil;
import com.panguso.datatransfer.util.SQLUtil;

/**
 * @author liubing
 *
 */
/**
 * @author liubing
 *
 */
public class CommonDataTransferJob implements Job {
	private static final Logger LOG = LoggerFactory.getLogger(CommonDataTransferJob.class);
//	private Logger resultlog = LoggerFactory.getLogger("result");
	private char[] cmdchar = null;
	protected String prefix = null;
	protected JobConf jobConf = null;
	protected String jobName = null;
	
	private boolean waitForJobEnable = false;
	private Set<String> waitForJobSet = null;
	
	/**
	 * 设置接入过程中的执行命令
	 * @param cmd 命令参数
	 */
	public void setCMD(String cmd) {
		if ((cmd == null) || ("".equals(cmd.trim()))
				|| (cmd.trim().length() != Common.CMDLEN)) {
			cmd = Common.DEFAULTCMD;
		}
		this.cmdchar = cmd.toCharArray();
	}
	
	/**
	 * 设置XPath前缀，用于读取配置文件
	 * @param prefix 前缀
	 */
	protected void setPrefix(String prefix) {
		if (CommonUtil.isNullOrEmpty(prefix)) {
			LOG.error("prefix in CommonDataTransferJob:setPrefix is empty, please check!");
			System.exit(-1);
		}
		this.prefix = prefix;
	}
	
	/**
	 * 获取数据下载类实例
	 * @return 数据下载类对象
	 */
	protected CommonDownloadService getDownloadService() {
		if (null == jobConf) {
			LOG.error("init variables error，cannot get a DownloadService instance！");
			return null;
		}
		//从config.xml中读取配置的下载类，并实例化对象
		String downloadClass = jobConf.getDownloadClass();
		if (CommonUtil.isNullOrEmpty(downloadClass)) {
			LOG.error("ERROR: get downloadClass failed, please check config.xml downloadClass tag");
			return null;
		}
		//用类名进行实例化
		LOG.debug("download class name : " + downloadClass);
		CommonDownloadService downloadService = null;
		try {
			downloadService = (CommonDownloadService) Class.forName(downloadClass).newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		if (downloadService != null) {
			downloadService.setPrefix(prefix);
			LOG.info("get instance of " + downloadClass + " success!");
			return downloadService;
		} else {
			LOG.error("get instance of CommonDownloadService error, please check download_class tag of the config.xml");
		}
		return null;
	}

	/**
	 * 获取一个数据导入类实例
	 * @return 数据导入类对象
	 */
	protected CommonImportService getImportService() {
		if (null == jobConf) {
			LOG.error("prefix is empty or null，we cannot get a CommonImportService instance！");
			return null;
		}
		//从config.xml中读取配置的数据导入类，并实例化对象
		String importClass = jobConf.getImportClass();
		if (CommonUtil.isNullOrEmpty(importClass)) {
			LOG.error("ERROR: get importClass failed, please check config.xml importClass tag");
			return null;
		}
		LOG.debug("import class name : " + importClass);
		//利用导入类名进行实例化
		CommonImportService importService = null;
		try {
			importService = (CommonImportService) Class.forName(importClass).newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		if (importService != null) {
			importService.setPrefix(prefix);
			LOG.info("get instance of " + importClass + " success!");
			return importService;
		} else {
			LOG.error("get instance of CommonImportService error, please check import_class tag of the config.xml");
		}
		return null;
	}

	
	/**
	 * 获取索引类的实例
	 * @return 索引类的实例
	 */
	protected CommonService getIndexService() {
		if (null == jobConf) {
			LOG.error("prefix is empty or null，we cannot get a IndexService instance！");
			return null;
		}
		//从config.xml中读取配置的索引类，并实例化对象
		String indexClass = jobConf.getIndexClass();
		if (CommonUtil.isNullOrEmpty(indexClass)) {
			LOG.error("ERROR: get indexClass failed, please check config.xml indexClass tag");
			return null;
		}
		LOG.debug("index class name : " + indexClass);
		
		CommonIndexService indexService = null;
		try {
			indexService = (CommonIndexService) Class.forName(indexClass).newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		if (indexService != null) {
			indexService.setPrefix(prefix);
			LOG.info("get instance of " + indexClass + " success!");
			return indexService;
		} else {
			LOG.error("get instance of CommonImportService error, please check import_class tag of the config.xml");
		}
		return null;
	}
	
	/**
	 * @return
	 */
	protected boolean beforeDownload() {
		return true;
	}

	/**
	 * @return
	 */
	protected boolean afterDownload() {
		return true;
	}

	/**
	 * @return
	 */
	protected boolean beforeImport() {
		return true;
	}

	/**
	 * @return
	 */
	protected boolean afterImport() {
		return true;
	}

	/**
	 * @return
	 */
	protected boolean beforeIndex() {
		return true;
	}

	/**
	 * @return
	 */
	protected boolean afterIndex() {
		return true;
	}

	
	/**
	 * 初始化参数
	 */
	private void init() {
		//通过prefix获取JobConf
		this.jobConf = GlobalVariable.getJobConf(prefix);
		if (null == jobConf) {
			LOG.warn("get jobConf failed，please check initVariables method！");
			System.exit(-1);
		}
		
		//获取并设置cmd
		setCMD(jobConf.getCmd());
		jobName = jobConf.getJobName();
		
		//针对索引job，检查等待非索引job列表
		if (CommonUtil.isIndexJob(jobName)) {
			//获取本索引job对应的IndexConf
			IndexConf indexConf = jobConf.getIndexConf();
			//获取本索引job需要等待的接入的job集合
			waitForJobEnable = indexConf.getWaitForJobEnable();
			waitForJobSet = indexConf.getWaitForJobSet();
		}
	}
	
	/**
	 * Job执行的入口函数
	 * @param context 上下文
	 */
	@Override
	public void execute(JobExecutionContext context) {
		//设置prefix
		String jobPrefix = context.getJobDetail().getGroup();
//		LOG.debug("job prefix: {}", jobPrefix);
		setPrefix(jobPrefix);
		
		//初始化
		init();
		
		//当前执行条件检查
		if (CommonUtil.isBigUnIndexJob(jobName)) {
			//全量非索引job
			if (!checkCondition4BigUnIndexjob()) {
				return;
			}
		} else if (CommonUtil.isDyUnIndexJob(jobName)) {
			//增量非索引job
			if (!checkCondition4DyUnIndexjob()) {
				return;
			}				
			//清空goods_delete表
			if (!cleanGoodsDeleteTable()) {
				LOG.error("{} clean goods_delete table failed!", jobName);
				System.exit(-1);
			}	
		} else if (CommonUtil.isBigIndexJob(jobName)) {
			if (!checkCondition4BigIndexjob()) {
				return;
			}
		} else if (CommonUtil.isDyIndexJob(jobName)) {
			if (!checkCondition4DyIndexjob()) {
				return;
			}
		}

		//执行任务
		doService();
		//删除doing文件
		FileUtil.removeFile(FileUtil.getDoingFilePath(jobName));
	}

	/**
	 * @function 增量非索引job，每次接入之前要清goods_delete中相关type的数据
	 * @return   执行结果
	 */
	private boolean cleanGoodsDeleteTable() {
//		//获取tableIndextypeMap
//		Set<String> indexTypeSet = (Set<String>) jobConf.getTableIndextypeMap().values();
//		if (null == indexTypeSet) {
//			LOG.error("get indexTypeSet for clear goods_delete table failed!");
//			return false;
//		}
//		
		String table = "goods_delete";
		final String delSql = "delete from goods_delete where type = 'type_replace'";
		List<String> delSqlList = new ArrayList<String>();
		//生成与本job相关的删除goods_delete中数据的SQL语句
		for (String indexType : jobConf.getTableIndextypeMap().values()) {
			delSqlList.add(delSql.replace("type_replace", indexType));
		}
		
		HashMap<String, TableConf> tableConfMap = GlobalVariable.getTableConfMap();
		if (null == tableConfMap || !tableConfMap.containsKey(table)) {
			LOG.error("TableConfMap is null or it does not contains {}", table);
			return false;
		}
		
		return SQLUtil.executeSql(delSqlList, tableConfMap.get(table).getDs());
	}

	/**
	 * @function 检查当前环境是否适合全量非索引job运行
	 * @return 	   检查结果
	 */
	private boolean checkCondition4BigUnIndexjob() {
		//若已经接入切未进行索引则推出（done文件存在）
		String doneFilePath = FileUtil.getDoneFilePath(jobName);
		if (FileUtil.isFileExist(doneFilePath)) {
			LOG.info("{} job have done, and it is not be indexed", jobName);
			return false;
		}
		
		//如果该job已经被调度且还未执行结束，则退出
		String doingFilePath = FileUtil.getDoingFilePath(jobName);
		if (FileUtil.isFileExist(doingFilePath)) {
			LOG.info("{} job have launched, cannot lanuch another", jobName);
			return false;
		} else {
			FileUtil.createDoingFile(jobName);
		}
		return true;
	}
	
	/**
	 * 检查当前环境是否适合增量非索引job运行
	 * @return 检查结果
	 */
	private boolean checkCondition4DyUnIndexjob() {
		String jobShortName = FileUtil.getShortJobName(jobName);
		String bigJobName = Common.BIG + jobShortName;
		//检查是否当前有本job对应的增量、全量job正在进行，或者已经完成
		String dyDoneFilePath   = FileUtil.getDoneFilePath(jobName);
		String dyDoingFilePath  = FileUtil.getDoingFilePath(jobName);
		String bigDoingFilePath = FileUtil.getDoingFilePath(bigJobName);
		if (FileUtil.isFileExist(dyDoneFilePath) || FileUtil.isFileExist(dyDoingFilePath)
				|| FileUtil.isFileExist(bigDoingFilePath)) {
			LOG.warn("{} have done or some job is doing, cannot lanuch {}", jobName, jobName);
			return false;
		}
		
		//检查当前是否有索引job正在进行
		List<String> allIndexList = GlobalVariable.getAllIndexJob();
		if (null == allIndexList) {
			LOG.error("get index job error, check GlobalVariable:getAllIndexJob");
			System.exit(-1);
		}
		//遍历所有index job 检查是否正在进行
		for (String indexJob : allIndexList) {
			String doingFilePath = FileUtil.getDoingFilePath(indexJob);
			if (FileUtil.isFileExist(doingFilePath)) {
				LOG.warn("some index job is doing, cannot launch {}", jobName);
				return false;
			}
		} 
		
		FileUtil.createDoingFile(jobName);
		return true;
	}
	
	/**
	 * 检查当前环境是否适合增量索引job运行
	 * @return 检查结果
	 */
	private boolean checkCondition4DyIndexjob() {
		//获取去除前缀的index的名称
		String indexShortName = FileUtil.getShortJobName(jobName);		
		String bigIndexName = Common.BIG + indexShortName;
		String dyIndexName = jobName;
		
		String bigIndexDoingFilePath = FileUtil.getDoingFilePath(bigIndexName);
		String dyIndexDoingFilePath = FileUtil.getDoingFilePath(dyIndexName);
		
		//判断当前是否有index(任意)正在进行
		if (FileUtil.isFileExist(bigIndexDoingFilePath) 
				|| FileUtil.isFileExist(dyIndexDoingFilePath)) {
			LOG.info("have index job is doing, cannot lanuch {}", jobName);
			return false;
		} else {
			FileUtil.createDoingFile(jobName);
		}
		
		//查看当前是否需要等待一些接入job
		if (waitForJobEnable) {
			//遍历要等待的job，查看是否已经完成
			for (String job : waitForJobSet) {
				String doneFile = FileUtil.getDoneFilePath(job);
				if (!FileUtil.isFileExist(doneFile)) {
					LOG.debug("doneFile {} of {} is not exist, return", job, doneFile);
					//如果有等待job没有完成，则退出
					FileUtil.removeFile(FileUtil.getDoingFilePath(jobName));
					return false;
				}
			}	
		}
		return true;
	}
	
	
	/**
	 * 检查当前环境是否适合全量索引job运行
	 * @return 检查结果
	 */
	private boolean checkCondition4BigIndexjob() {
		//jobName 是  big  index
		boolean hasReady = false;
		
		//获取去除前缀的index的名称
		String bigIndexJobName = jobName;
		String bigIndexDoingFilePath = FileUtil.getDoingFilePath(bigIndexJobName);
		
		//全局所有增量job
		HashMap<String, JobConf> dyJobMap = GlobalVariable.getDyjobConfMap();
		
		//判断当前是否有bigindex正在进行
		if (FileUtil.isFileExist(bigIndexDoingFilePath)) {
			LOG.info("{} is doing, cannot lanuch another", jobName);
			return hasReady;
		} else {
			FileUtil.createDoingFile(jobName);
		}
		
		//检测当前是否有增量job正在进行
		int tried = 0;
		boolean isSomeDyjobDoing = false;
		do {
			isSomeDyjobDoing = false;
			//遍历增量job，查看其对应的doing文件是否存在
			for (String dyJob : dyJobMap.keySet()) {
				String doingFilePath = FileUtil.getDoingFilePath(dyJob);
				if (FileUtil.isFileExist(doingFilePath)) {
					isSomeDyjobDoing = true;
					break;
				}
			}
			
			//如果有任何一个增量job的doing文件存在，则需要等待其完成
			if (isSomeDyjobDoing) {
				LOG.warn("some dyjob is doing, {} must wait", jobName);
				if (tried++ < Common.INDEX_MAX_SLEEP_CNT) {
					try {
						Thread.sleep(Common.INDEX_PRE_SLEEP_TIME);
					} catch (InterruptedException e) {
						LOG.error("{} sleep error when waiting for other dyjob");
						return hasReady;
					}
				} else {
					LOG.error("{} have wait max time, but still some dyjob is running");
					return hasReady;
				}
			}
		} while(isSomeDyjobDoing);
		
		//检查是否需要等待接入job
		if (waitForJobEnable) {
			//是否正在等待job
			boolean waitingJob = true;
			tried = 0;
			while (waitingJob) {
				waitingJob = false;
				for (String job : waitForJobSet) {
					String doneFile = FileUtil.getDoneFilePath(job);
					if (!FileUtil.isFileExist(doneFile)) {
						LOG.warn("{} need wait, doneFile path of {} is not exist", jobName, job);
						waitingJob = true;
						break;
					}
				}
				
				//如果有等待的job的done文件不存在则要等待一段时间
				if (waitingJob) {
					if (tried++ < Common.INDEX_MAX_SLEEP_CNT) {
						try {
							Thread.sleep(Common.INDEX_PRE_SLEEP_TIME);
						} catch (InterruptedException e) {
							LOG.error("{} sleep error when waiting for waitjobs done");
							return hasReady;
						}
					} else {
						LOG.error("{} have wait max time, but still some waitjob is not done");
						return hasReady;
					}
				}
			}
		}
		hasReady = true;
		
		return hasReady;
	}
	
	
	/**
	 * 按照配置的命令值，依次执行下载、入库、索引步骤
	 */
	private void doService() {
		long startTime = System.currentTimeMillis();
		//下载前的预处理操作
		boolean result = true;
		result = beforeDownload();
		if (!result) {
			LOG.warn("before Download ret false. exit job");
			return;
		}
		
		// 下载文件操作
		if (result && cmdchar.length >= 1 && cmdchar[0] == '1') {
			long downloadstart = System.currentTimeMillis();
			CommonDownloadService downloadservice = getDownloadService();
			if (downloadservice != null) {
				result = downloadservice.doService();
				if (result) {
					long downloadEnd = System.currentTimeMillis();
					LOG.info("download process has finished, and total cost " 
								+ DateUtil.getReadableTime(downloadEnd - downloadstart));
				} else {
					LOG.info("download process failed or There are no files need to download!!");
					return;
				}
			}
		} else {
			LOG.info("cmd is {}, no need do download process of {}", cmdchar, jobName);
		}
		//下载后的处理
		result = afterDownload();
		//导入数据库前的预处理
		result = beforeImport();

		// 导入数据到数据库
		if (this.cmdchar.length >= 2 && (this.cmdchar[1] == '1') && (result)) {
			long indbstart = System.currentTimeMillis();
			CommonImportService importService = getImportService();
			if (importService != null) {
				result = importService.doService();
				if (result) {
					long indbEnd = System.currentTimeMillis();
					LOG.info("import files to DB total cost " + DateUtil.getReadableTime(indbEnd - indbstart));
				} else {
					LOG.info("import files into DB failed!!");
					return;
				}
			} else {
				LOG.info("import service instance is null!!");
			}
		} else {
			LOG.info("cmd is {}, no need do import process of {}", cmdchar, jobName);
		}
		// 导入数据库后的处理
		result = afterImport();
		//建立索引前的预处理
		result = beforeIndex();

		// 建立索引操作
		if (this.cmdchar.length >= 3 && (this.cmdchar[2] == '1') && (result)) {
			long indexStartTime = System.currentTimeMillis();
			CommonService indexService = getIndexService();
			if (indexService != null) {
				result = indexService.doService();
				if (result) {
					long indexEndTime = System.currentTimeMillis();
					LOG.info("create index success, total cost {}", DateUtil.getReadableTime(indexEndTime - indexStartTime));
				}
			}
		} else {
			LOG.info("cmd is {}, no need do index process of {}", cmdchar, jobName);
		}
		//建立索引完成后的处理
		afterIndex();
		
		long endTime = System.currentTimeMillis();
		LOG.info("data transfer process of {} total cost: {}", jobName, DateUtil.getReadableTime(endTime - startTime));
	}	
}