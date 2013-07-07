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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.conf.JobConf;
import com.panguso.datatransfer.conf.TableConf;
import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.GlobalVariable;
import com.panguso.datatransfer.util.CommonUtil;
import com.panguso.datatransfer.util.DateUtil;
import com.panguso.datatransfer.util.FileUtil;
import com.panguso.datatransfer.util.IOUtil;
import com.panguso.datatransfer.util.SQLUtil;

/**
 * 数据接入公共基类
 * @author liubing
 */
public class CommonImportService implements CommonService {
	private static final Logger LOG = LoggerFactory.getLogger(CommonImportService.class);
	protected String prefix = "";		//job前缀
	protected JobConf jobConf = null;	//job配置信息
	protected Map<String, TableConf> tableConfMap = null;	//数据表名与表配置信息的映射 
	protected String[] fields = null;	//数据表的字段
	protected String writeDownloadFilePath = null;	//下载的文件列表要写入的文件路径
	
	/**
	 * @function  设置job的前缀
	 * @param prefix 前缀
	 */
	@Override
	public void setPrefix(String prefix) {
		if (CommonUtil.isNullOrEmpty(prefix)) {
			LOG.error("prefix is null or empty, cannot init ImportService correctly!");
			return;
		}
		this.prefix = prefix;
	}
	

	/**
	 * @function 接入数据构成入口
	 * @return   接入过程是否成功
	 */
	@Override
	public boolean doService() {
		//初始化参数
		init();
		
		long start = System.currentTimeMillis();
		//获取文件列表与各自对应的数据表的映射
		Map<String, String> fileTableMap = getFileTableMap();
		
		//验证数据接入表的正确性，并有条件的[全量方式]进行清空
		if (!verifyTotable(fileTableMap)) {
			LOG.error("verify to tables error!");
			System.exit(-1);
		}
		
		//接入数据到数据库
		boolean importRet = importData(fileTableMap);

		//如果导入数据成功
		if (importRet) {
			// 删除本job的下载列表
			removeDownloadFile();
			//为本job创建done文件
			FileUtil.createDoneFile(jobConf.getJobName());
		}
		
		//计算该数据导入过程耗时
		long end = System.currentTimeMillis();
		LOG.info("Import process success, and total cost " + DateUtil.getReadableTime(end - start));
		return true;
	}
	
	
	/**
	 * @function 初始化各数据成员
	 */
	protected void init() {		
	}
	
	/**
	 * @function 获取下载的各文件与其要入库到的数据表名的映射关系
	 * @return	 Map<FilePath, tableName>
	 */
	protected Map<String, String> getFileTableMap() {
		return new HashMap<String, String>();
	}
	
	
	/**
	 * @function   验证MD5文件是否存在
	 * @param path 待验证的路径
	 * @return	         验证结果
	 */
	protected boolean checkMD5OfFile(String path) {
		return true;
	}
	
	/**
	 * @param 将文件中数据导入到DB中
	 * @return
	 */
	private boolean importData(Map<String, String> fileTableMap) {
		
		if (null == fileTableMap) {
			LOG.error("fileTableMap is null, no need to do import process of {}", jobConf.getJobName());
			return false;
		}
		
		boolean importRet = true;
		for (String filePath : fileTableMap.keySet()) {
			//忽略MD5文件
			if (filePath.endsWith(Common.MD5SUFFIX)) {
				continue;
			}
			LOG.info("Begin to handle {}", filePath);
			long start = System.currentTimeMillis();
			//对数据文件进行MD5检查
			if (!checkMD5OfFile(filePath)) {
				LOG.error("MD5 checking error of file {}, please check!", filePath);
				continue;
			}
			//读取文件的所有行数据
			String encode = jobConf.getEncode();
			List<String> lines = IOUtil.readFile(filePath, encode);
			if (CommonUtil.isNullOrEmpty(lines)) {
				LOG.error("There are no content in file {}", filePath);
				continue;
			}
			//转换数据成sql语句集合
			String tableName = fileTableMap.get(filePath);
			String fileName = FileUtil.getShortFileName(filePath);
			List<String> sqlList = createSql(lines, fileName, tableName);
			if (CommonUtil.isNullOrEmpty(sqlList)) {
				LOG.error("sql statement list created from {} is null or empty!", fileName);
				continue;
			}
			LOG.info("create total {} sql statements from {}", sqlList.size(), filePath);
			
			//获取插入数据表对应的数据库连接信息
			TableConf tableConf = tableConfMap.get(tableName);
			if (null == tableConf) {
				LOG.error("import {} failed, no tableConf for {}", filePath, tableName);
				importRet = false;
				break;
			}
			
			//执行数据库插入操作
			if (!SQLUtil.executeSql(sqlList, tableConf.getDs())) {
				LOG.error("import {} into DB failed!", filePath);
				importRet = false;
				break;
			}
			
			saveLatestStamp(filePath);
			long end = System.currentTimeMillis();
			LOG.info("execute sql statement for file {} cost {}", filePath,  DateUtil.getReadableTime(end - start));
		}
		
		return importRet;
	}


	/**
	 * @function 将文件中的每行数据转化成入库的SQL语句
	 * @param lines     从文件中读取的每行数据
	 * @param fileName  读取数据的文件的文件名
	 * @param tableName 要入库的表名
	 * @return
	 */
	protected List<String> createSql(List<String> lines, String fileName, String tableName) {
		return new ArrayList<String>();
	}
	
	
	/**
	 * @function 删除保存下载列表的文件
	 */
	protected void removeDownloadFile() {
		FileUtil.removeFile(writeDownloadFilePath);
	}
	
	
	/**
	 * @function 保存文件上次导入数据库的最后时间和序号
	 * @param    filePath 文件路径
	 */
	protected void saveLatestStamp(String filePath) {
	}
	

	/**
	 * @function 数据入库前，验证接入数据表（to table）的正确性，全量接入时还删除表中数据
	 * @param 	 fileTableMap 下载文件与待入库表的映射
	 * @return   接入表的验证结果
	 */
	protected boolean verifyTotable(Map<String, String> fileTableMap) {
		if (null == fileTableMap) {
			LOG.error("fileTableMap is null, no need to do import process of {}", jobConf.getJobName());
			return false;
		}
		
		if (fileTableMap.isEmpty()) {
			LOG.warn("fileTableMap is empty, There are no files have download!");
			return true;
		}
		
		//数据表名添加到toTableList，用于接入前删除表中所有数据
		List<String> toTableList = new ArrayList<String>();
		for (String tableName : fileTableMap.values()) {
			toTableList.add(tableName);
		}
		
		if (!GlobalVariable.isAllTablesAvailable(toTableList)) {
			LOG.error("some from table is unavailable, please check {}", toTableList.toString());
			return false;
		}
		
		//如果是全量接入job，入库前要先清空全量表
		if (this.prefix.startsWith(Common.BIG)) {
			if (!cleanToTable(toTableList)) {
				LOG.error("clear toTable list error before import data!");
				return false;
			}
		}
		return true;
	}
	
	/**
	 * @function 数据接入之前先清空全量数据表
	 * @return	  清除结果
	 */
	private boolean cleanToTable(List<String> toTableList) {
		if (CommonUtil.isNullOrEmpty(toTableList)) {
			LOG.error("to table list is null or empty");
			System.exit(-1);
		}

		String toTable = null;
		final String delSql = "delete from table_replace";
		List<String> sqlList = new ArrayList<String>();

		//待接入的数据表对应的删除sql语句
		for (String table : toTableList) {
			toTable = table;
			sqlList.add(delSql.replace("table_replace", toTable));
		}
		//返回SQL处理结果
		return SQLUtil.executeSql(sqlList, tableConfMap.get(toTable).getDs());
	}
}