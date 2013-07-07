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

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.conf.FtpConf;
import com.panguso.datatransfer.conf.FtpDownloadConf;
import com.panguso.datatransfer.conf.TableConf;
import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.GlobalVariable;
import com.panguso.datatransfer.util.CommonUtil;
import com.panguso.datatransfer.util.FileUtil;
import com.panguso.datatransfer.util.IOUtil;
import com.panguso.datatransfer.util.MD5Util;


/**
 * 全量FTP数据导入类
 * @author liubing
 *
 */
public class BigFTPImportService extends CommonImportService {

	private static final Logger LOG = LoggerFactory.getLogger(BigFTPImportService.class);
 
	private FtpConf ftpConf = null;
	private FtpDownloadConf ftpDownloadConf = null;
		
	/**
	 * 初始化函数
	 * 用于初始化各成员变量等
	 */
	@Override
	protected void init() {
		if (CommonUtil.isNullOrEmpty(prefix)) {
			LOG.error("prefix is null or empty, so cannot init variables!!");
			System.exit(-1);
		}
		//初始化job信息映射map
		jobConf = GlobalVariable.getJobConf(prefix);
		if (null == jobConf) {
			LOG.error("jobConf for {} is null, please check config.xml", prefix);
			System.exit(-1);
		}
		//初始化数据表信息映射map
		tableConfMap = GlobalVariable.getTableConfMap();
		//初始化FTP相关信息
		ftpConf = jobConf.getFtpConf();
		if (null == ftpConf) {
			LOG.error("ftpConf for {} is null, please check config.xml", prefix);
			System.exit(-1);
		}
		//初始化FtpDownloadConf
		ftpDownloadConf = ftpConf.getFtpDownloadConf();
	}


	/**
	 * 获取下载的本地文件的路径名与待接入到的数据表名的映射关系
	 */
	@Override
	protected Map<String, String> getFileTableMap() {
		Map<String, String> pathTableMap = new LinkedHashMap<String, String>();
		//获取下载的文件的路径列表
		List<String> fileList = getFileList();
		if (CommonUtil.isNullOrEmpty(fileList)) {
			LOG.error("get download files list error, please the path!");
			return null;
		}
		//获取配置文件中文件名与数据表名的对应关系	
		Map<String, String> fileTableMap = ftpDownloadConf.getFileTableMap();
		for (String filePath : fileList) {
			File file = new File(filePath);
			//从文件的绝对路径中提取短文件名，如/***/20120514_androidGame_1.txt 中提取androidGame
			String fileName = FileUtil.getShortFileName(file.getName());
			String tableName = fileTableMap.get(fileName);
			if (!CommonUtil.isNullOrEmpty(tableName)) {
				LOG.debug(filePath + "---->" + tableName);
				pathTableMap.put(filePath, tableName);
			}
		}
				
		return pathTableMap;
	}


	/**
	 * 获取下载的文件的路径列表
	 * @return 本job已经下载的各文件的列表
	 */
	private List<String> getFileList() {
		//获取本项目的本地根目录
		String localRootPath = ftpDownloadConf.getLocalPath();
		if (!localRootPath.endsWith(File.separator)) {
			localRootPath += File.separator;
		}
		//获取记录了下载列表的文件路径
		writeDownloadFilePath = localRootPath + prefix + Common.DOWNLOADFILE;
		LOG.debug("writeDownloadFilePath : " + writeDownloadFilePath);
		//读取文件内容，即下载的文件的路径列表
		return IOUtil.readFile(writeDownloadFilePath);
	}
		

	/**
	 * 检查数据文件所在目录下是否有对应MD5文件，并检查是否正确
	 * @param path 待检查的数据文件的路径
	 * @return 验证结果
	 */
	@Override
	protected boolean checkMD5OfFile(String path) {
		//验证文件路径的正确性
		if (CommonUtil.isNullOrEmpty(path)) {
			LOG.error("The path which checke md5 is null or empty!");
			return false;
		}
		//获取MD5文件 和 数据文件 的后缀名格式
		String md5Suffix = Common.DOT + Common.MD5SUFFIX;
		String dataSuffix = Common.DOT + ftpDownloadConf.getDataFileSuffix();
		if (!path.endsWith(dataSuffix)) {
			LOG.error("suffix format of file " + path + " is error, please check!");
			return false;
		}
		//获取数据文件对应的MD5文件路径
		String md5File = path.replace(dataSuffix, md5Suffix);
		//读取MD5文件的内容
		List<String> lines = IOUtil.readFile(md5File);
		if (CommonUtil.isNullOrEmpty(lines)) {
			LOG.error("Content of " + md5File + " is empty");
			return false;
		}
		//数据文件的计算的MD5值、对应MD5文件中记录的MD5值
		String computeMD5 = null, storeMD5 = null;
		try {
			computeMD5 = MD5Util.getFileMD5String(new File(path)).trim();
			storeMD5 = lines.get(0).trim();			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		//验证数据文件的MD5计算值是否与MD5文件里的值一致
		return computeMD5.equalsIgnoreCase(storeMD5);
	}

	/** 
	 * 将从数据文件中读取的数据转换成sql语句
	 * @param lines 从数据文件中读取的每行数据的列表
	 * @param fileName lines数据来自文件的文件名称
	 * @param tableName 待接入到数据库的文件名
	 * @return 生成的SQL语句
	 */
	@Override
	protected List<String> createSql(List<String> lines, String fileName, String tableName) {
		//判断参数的正确性
		if (CommonUtil.isNullOrEmpty(lines)) {
			LOG.error("lines is null or empty, please check file {}", fileName);
			return null;
		}
		if (CommonUtil.isNullOrEmpty(fileName) || CommonUtil.isNullOrEmpty(tableName)) {
			LOG.error("fileName/tableName is null or empty, file:{}  table:{}", fileName, tableName);
			return null;
		}
		
		//获取文件接入数据时要忽略的字段集合
		HashSet<Integer> skipSet = null;
		if (ftpDownloadConf.getFileSkipMap() != null) {
			skipSet = ftpDownloadConf.getFileSkipMap().get(fileName);
		}
		
		//获取tableName名称对应表的配置信息
		TableConf tableConf = tableConfMap.get(tableName);
		if (null == tableConf) {
			LOG.error("tableConf of {} is null in BigFTPImportService:createBigSql", tableName);
			return null;
		}
		
		//获取待接入数据表的字段集合
		String[] fields = tableConf.getFieldConf().getFields();
		if (CommonUtil.isNullOrEmpty(fields)) {
			LOG.error("fields of {} is null or empty in BigFTPImportService:createBigSql", tableName);
			return null;
		}
		
		//获取待接入数据表的字段长度等信息
		Map<String, Integer> fieldLenMap = tableConf.getFieldLenMap();
		if (null == fieldLenMap) {
			LOG.error("fieldLenMap of table " + tableName + " is null in method createBigSql");
			return null;
		}
		
		//获取文件的字段分隔符
		String separator = jobConf.getFieldSeparator(); 
		List<String> sqlList = new ArrayList<String>();
		for (int idx = 0; idx < lines.size(); ++idx) {
			//从数据表中获取原始行
			String line = lines.get(idx);
			String[] rawDatas = line.split(separator);
			if (CommonUtil.isNullOrEmpty(rawDatas)) {
				continue;
			}
			
			//获取该行数据的primarykey,并判断其正确性
			String primaryKey = getPrimaryKey(rawDatas, fileName);
			if (CommonUtil.isNullOrEmpty(primaryKey)) {
				LOG.error("data is {}, not contains primary key!", line);
				continue;
			}
			
			//去除要过略的字段
			String[] datas = null;
			int rawSize = rawDatas.length, rawIdx = 0, newIdx = 0;
			//如果郭略字段集合为空，则新数据与元数据保持一致
			if (CommonUtil.isNullOrEmpty(skipSet)) {
				datas = rawDatas;
				newIdx = rawSize;
			} else { 
				datas = new String[rawSize];
				for (rawIdx = 0; rawIdx < rawSize; ++rawIdx) {
					if (!skipSet.contains(rawIdx + 1)) {
						datas[newIdx++] = rawDatas[rawIdx];	
					}
				}
			}
						
			//拼接插入SQL语句
			StringBuilder strSql = new StringBuilder("insert into ");
			strSql.append(tableName).append("(");
			
			int fieldSize = Math.min(newIdx, fields.length);
			for (int n = 0; n < fieldSize; n++) {
				if (!CommonUtil.isNullOrEmpty(datas[n])) {
					strSql.append("`" + fields[n] + "`");
					strSql.append(",");
				}
			}
			strSql.deleteCharAt(strSql.length() - 1);

			strSql.append(") values(");
			for (int n = 0; n < fieldSize; n++) {
				if (CommonUtil.isNullOrEmpty(datas[n])) {
					continue;
				}
				//检查字段长度
				String key = fields[n];
				String value = datas[n];
				if (!CommonUtil.isNullOrEmpty(fieldLenMap) && fieldLenMap.containsKey(key)) {
					int limitLen = fieldLenMap.get(key);
					if (value.length() > limitLen) {
						value = value.substring(0, fieldLenMap.get(key));
					}
				}
				strSql.append("\"").append(value.replaceAll("\\\\", "")
									.replaceAll("\"", "\\\"")).append("\"");
				strSql.append(",");
			}
			strSql.deleteCharAt(strSql.length() - 1);
			strSql.append(")");

			sqlList.add(strSql.toString());	
		}
		return sqlList;
	}
	
	
	/**
	 * @function 获取fileName文件的其中一行数据datas中获取主键值
	 * @param datas 代表文件中一行数据的字段值的数组
	 * @param fileName 文件名，即datas就是从这个文件中获取的
	 * @return
	 */
	private String getPrimaryKey(String[] datas, String fileName) {
		//验证参数的可用性
		if (CommonUtil.isNullOrEmpty(datas) || CommonUtil.isNullOrEmpty(fileName)) {
			LOG.error("getPrimaryKey error! invalid paraments!");
			return null;
		}
		//获取fileName文件对应的主键序号
		int primaryKeyIdx = ftpDownloadConf.getFilePrimarykeyIdxMap().get(fileName);
		if (primaryKeyIdx > datas.length) {
			LOG.error("getPrimaryKey error, index of primaryKey is out of boundary!");
			return null;
		}
		return datas[primaryKeyIdx - 1];
	}
}
