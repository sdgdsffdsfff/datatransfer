/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.conf.IndexConf;
import com.panguso.datatransfer.conf.JobConf;
import com.panguso.datatransfer.conf.TableConf;
import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.GlobalVariable;
import com.panguso.datatransfer.util.CommonUtil;
import com.panguso.datatransfer.util.DateUtil;
import com.panguso.datatransfer.util.FileUtil;
import com.panguso.datatransfer.util.SQLUtil;

/**
 * @author liubing
 *
 */
public class CommonIndexService implements CommonService {
	private static final Logger LOG = LoggerFactory.getLogger(CommonIndexService.class);
	protected String prefix = null;
	
	//获取本index需要等待的job列表
	private JobConf jobConf = null;
	private IndexConf indexConf = null;
	private boolean waitForJobEnable = false;
	private Set<String> waitForJobSet = null;
	private Set<String> involveTableSet = null;  //全量索引需要转移数据的表集合
	
	/**
	 * 初始化
	 */
	public void init() {		
		//获取本索引job对应的JobConf
		jobConf = GlobalVariable.getJobConf(prefix);
		if (null == jobConf) {
			LOG.error("{} is not an available job, cannot get its jobConf");
			System.exit(-1);
		}
		//获取本索引job对应的IndexConf
		indexConf = jobConf.getIndexConf();
		//获取本索引job需要等待的接入的job集合
		waitForJobEnable = indexConf.getWaitForJobEnable();
		waitForJobSet = indexConf.getWaitForJobSet();
	}
	
	/**
	 * 接入数据构成入口
	 * @return 接入过程是否成功
	 */
	@Override
	public boolean doService() {		
		//验证prefix参数的正确性
		if (CommonUtil.isNullOrEmpty(prefix)) {
			LOG.error("prefix in empty, cannot get JobConf in CommonIndexService");
			System.exit(-1);
		}
		
		init();
//		//验证是否满足可以进行index操作的条件
//		boolean indexReady = hasReady();
//		if (!indexReady) {
//			LOG.info("condition is not be met, cannot do index process!");
//			return false;
//		}
		
		//条件满足，尅是建立索引
		long start = System.currentTimeMillis();	
		boolean createIndexRet = true;
		createIndexRet = createIndex();
		
		//如果索引建立成功
		if (createIndexRet) {
			//删除等待的job对应的done文件
			if (this.waitForJobEnable) {
				for (String jobName : waitForJobSet) {
					String doneFilePath = FileUtil.getDoneFilePath(jobName);
					FileUtil.removeFile(doneFilePath);
				}
			}
			//计算建立索引耗时
			long end = System.currentTimeMillis();
			LOG.info("Index process success, and total cost : " + DateUtil.getReadableTime(end - start));
		}
		
		//删除index正在进行的标识文件doing文件
		FileUtil.removeFile(FileUtil.getDoingFilePath(prefix));
		return createIndexRet;
	}
	

	/**
	 * 设置JOB前缀
	 * @param prefix 前缀
	 */
	@Override
	public void setPrefix(String prefix) {
		if (CommonUtil.isNullOrEmpty(prefix)) {
			LOG.error("prefix is null or empty, cannot init IndexService correctly!");
			return;
		}
		this.prefix = prefix;
	}
	
//	/**
//	 * @function 获取为job执行索引的脚本的路径
//	 * @return   执行index的脚本路径
//	 */
//	private String getIndexCmd() {
//		if (CommonUtil.isNullOrEmpty(prefix)) {
//			LOG.error("get create index shell cmd error, because prefix is null or empty!");
//			return null;
//		}
//		
//		String indexTag = null;
//		if (prefix.contains(Common.BIG)) {
//			indexTag = Common.BIGINDEX + Common.INDEXCMD;
//		} else if (prefix.contains(Common.DY)) {
//			indexTag = Common.DYJOBS + Common.INDEXCMD;
//		}
//		
//		String indexCMD = ConfigFactory.getString(indexTag, "");
//		LOG.debug("index shell path is {}", indexCMD);
//		return indexCMD;
//	}
	
//	/**
//	 * 判断当前环境是否满足可以建索引的条件
//	 * @return 判断结果
//	 */
//	protected boolean hasReady() {
//		boolean hasReady = false;
//		if (CommonUtil.isNullOrEmpty(prefix)) {
//			LOG.error("prefix is null or empty, cannot create index!");
//			return hasReady;
//		}
//		//获取去除前缀的index的名称
//		String indexShortName = FileUtil.getShortFileName(prefix);		
//		String bigIndexName = Common.BIG + indexShortName;
//		String dyIndexName = Common.DY + indexShortName;
//		LOG.debug("short name of {} is {}", prefix, indexShortName);
//		
//		String bigIndexDoingFilePath = FileUtil.getDoingFilePath(bigIndexName);
//		String dyIndexDoingFilePath = FileUtil.getDoingFilePath(dyIndexName);
//		
//		//全局所有增量job
//		HashMap<String, JobConf> dyJobMap = GlobalVariable.getDyjobConfMap();
//		
//		//如果是全量索引
//		if (prefix.contains(Common.BIG)) {
//			//判断当前是否有bigindex正在进行
//			if (FileUtil.isFileExist(bigIndexDoingFilePath)) {
//				LOG.info("{} is doing, cannot lanuch another", prefix);
//				return hasReady;
//			} else {
//				FileUtil.createDoingFile(prefix);
//			}
//			
//			//检测当前是否有增量job正在进行
//			int tried = 0;
//			boolean isSomeDyjobDoing = false;
//			do {
//				isSomeDyjobDoing = false;
//				//遍历增量job，查看其对应的doing文件是否存在
//				for (String dyJob : dyJobMap.keySet()) {
//					String doingFilePath = FileUtil.getDoingFilePath(dyJob);
//					if (FileUtil.isFileExist(doingFilePath)) {
//						isSomeDyjobDoing = true;
//						break;
//					}
//				}
//				
//				//如果有任何一个增量job的doing文件存在，则需要等待其完成
//				if (isSomeDyjobDoing) {
//					LOG.warn("some dyjob is doing, {} must wait", prefix);
//					if (tried++ < Common.INDEX_MAX_SLEEP_CNT) {
//						try {
//							Thread.sleep(Common.INDEX_PRE_SLEEP_TIME);
//						} catch (InterruptedException e) {
//							LOG.error("{} sleep error when waiting for other dyjob");
//							return hasReady;
//						}
//					} else {
//						LOG.error("{} have wait max time, but still some dyjob is running");
//						return hasReady;
//					}
//				}
//			} while(isSomeDyjobDoing);
//			
//			//检查是否需要等待接入job
//			if (waitForJobEnable) {
//				//是否正在等待job
//				boolean waitingJob = true;
//				tried = 0;
//				while (waitingJob) {
//					waitingJob = false;
//					for (String jobName : waitForJobSet) {
//						String doneFile = FileUtil.getDoneFilePath(jobName);
//						LOG.debug("doneFile path of {} is {}", jobName, doneFile);
//						if (!FileUtil.isFileExist(doneFile)) {
//							waitingJob = true;
//							break;
//						}
//					}
//					
//					//如果有等待的job的done文件不存在，仍需要等，休眠一段时间
//					if (waitingJob) {
//						if (tried++ < Common.INDEX_MAX_SLEEP_CNT) {
//							try {
//								Thread.sleep(Common.INDEX_PRE_SLEEP_TIME);
//							} catch (InterruptedException e) {
//								LOG.error("{} sleep error when waiting for waitjobs done");
//								return hasReady;
//							}
//						} else {
//							LOG.error("{} have wait max time, but still some waitjob is not done");
//							return hasReady;
//						}
//					}
//				}
//			}
//			hasReady = true;
//		} else if (prefix.contains(Common.DY)) {
//			//判断当前是否有index(任意)正在进行
//			if (FileUtil.isFileExist(bigIndexDoingFilePath) 
//					|| FileUtil.isFileExist(dyIndexDoingFilePath)) {
//				LOG.info("have index job is doing, cannot lanuch another", prefix);
//				return hasReady;
//			} else {
//				FileUtil.createDoingFile(prefix);
//			}
//			
//			//查看当前是否需要等待一些接入job
//			if (waitForJobEnable) {
//				//遍历要等待的job，查看是否已经完成
//				for (String jobName : waitForJobSet) {
//					String doneFile = FileUtil.getDoneFilePath(jobName);
//					LOG.debug("doneFile path of {} is {}", jobName, doneFile);
//					if (!FileUtil.isFileExist(doneFile)) {
//						//如果有等待job没有完成，则退出
//						return hasReady;
//					}
//				}	
//			}
//			hasReady = true;
//		}
//		return hasReady;
//	}
	
	
	/**
	 * 创建索引
	 * @return 创建索引成功与否
	 */
	protected boolean createIndex() {
		//获取执行索引需要的脚本的路径
		String indexCMD = jobConf.getIndexConf().getIndexCmd();
		if (!FileUtil.isFileExist(indexCMD)) {
			LOG.error("index path is unavailable! please check");
			System.exit(-1);
		}
			
		//创建全量索引需要将增量数据表中的数据转到全量表中来
		if (prefix.contains(Common.BIG)) {
			if (!transferDytableToBigtable()) {
				LOG.error("transfer data from dytable to bigtable error when doing {} job", prefix);
				System.exit(-1);
			}
		}
		
		
		//执行索引结果
		boolean indexRet = false;
		if (!CommonUtil.isNullOrEmpty(indexCMD)) {
			Runtime rt = Runtime.getRuntime();
			try {
				Process pro = rt.exec(indexCMD);
				int ret = pro.waitFor();
				if (ret != 0) {
					LOG.error("indexing for {} error", prefix);
				} else {
					LOG.info("indexing for {} success", prefix);
					indexRet = true;
				}
			} catch (Exception e) {
				LOG.error("indexing for {} error", prefix);
			}
		} else {
			LOG.error("indexing for {} error, shell cmd path is null or empty!", prefix);
		}
		return indexRet;
	}
	
	/**
	 * 建全量索引之前会把增量表中的数据转到全量表中
	 * @return
	 */
	private boolean transferDytableToBigtable() {
		final String delSql = "delete from table_replace";
		final String transferSql = "insert into to_table select * from from_table";
		
		involveTableSet = indexConf.getInvolveTableSet();
		if (CommonUtil.isNullOrEmpty(involveTableSet)) {
			LOG.warn("involveTableSet of {} is null or empty", prefix);
			return true;
//			System.exit(-1);
		}
		List<String> sqlList = new ArrayList<String>();
		//involveTableSet配置的必须都是全量表
		String toTable = null;
		for (String tableName : involveTableSet) {
			toTable = tableName;
			String fromTable = Common.DYTABLESUFFIX + tableName;
			LOG.debug("[index] from table: {}, to table: {}", fromTable, toTable);
			sqlList.add(transferSql.replace("from_table", fromTable).replace("to_table", toTable));
			sqlList.add(delSql.replace("table_replace", fromTable));
		}
		
		//执行数据转移SQL语句
		HashMap<String, TableConf> tableConfMap = GlobalVariable.getTableConfMap();
		if (CommonUtil.isNullOrEmpty(tableConfMap)) {
			LOG.error("get tableConfMap error when doing {} job", prefix);
			System.exit(-1);
		}		
		return SQLUtil.executeSql(sqlList, tableConfMap.get(toTable).getDs());
	}
	
	
}
