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
import java.util.Collections;
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
 * @author liubing
 *
 */
public class DyFTPImportService extends CommonImportService {

	private static final Logger LOG = LoggerFactory.getLogger(DyFTPImportService.class);
 
	private FtpConf ftpConf = null;
	private FtpDownloadConf ftpDownloadConf = null;
	private TableConf tableConf = null;
	private String fullToTable = null;
	private String dyToTable = null;
	private Map<String, Integer> fieldLenMap = null;
	
	//增加、删除、修改的base SQL语句
	private final String updateBaseSql = "update tableName set ";
	private final String addBaseSql = "insert into tableName(";
	private final String delBaseSql = "delete from tableName where primaryKey = \"";
	private final String insertGoodsDelSql = "insert into goods_delete values(\"type_replace\", \"goodsid_replace\")";
	
	
	//初始化各数据成员
	@Override
	protected void init() {
		if (CommonUtil.isNullOrEmpty(prefix)) {
			LOG.error("prefix is null or empty, so cannot init variables!!");
			System.exit(-1);
		}
		//初始化job信息映射map
		jobConf = GlobalVariable.getJobConf(prefix);
		if (null == jobConf) {
			LOG.error("jobConf for " + prefix + " is null, please check config.xml!!!");
			System.exit(-1);
		}
		//初始化数据表信息映射map
		tableConfMap = GlobalVariable.getTableConfMap();
		//初始化FTP相关信息
		ftpConf = jobConf.getFtpConf();
		ftpDownloadConf = ftpConf.getFtpDownloadConf();
	}

	/**
	 * 验证增量入库表和全量入库表的可用性
	 * @param toTable 待验证的表名（增量表名，以dy打头的表名）
	 * @return 验证结果
	 */
	private void verifyToTable(String toTable) {
		//验证数据表名的完整性
		if (CommonUtil.isNullOrEmpty(toTable)) {
			LOG.error("table name is null or empty, can not import data into db!");
			System.exit(-1);
		}
		//验证数据表名的正确性，FTP的增量接入配置的增量表名
		if (!toTable.startsWith(Common.DYTABLESUFFIX)) {
			LOG.error("table name is not started with dy_, can not import data into db!");
			System.exit(-1);
		}
		
		//验证增量表、全量表的正确性、合法性		
		this.dyToTable = toTable;
		this.fullToTable = Common.FULLTABLESUFFIX + toTable.substring(Common.DYTABLESUFFIX.length());
		if (!(GlobalVariable.isTableAvailable(fullToTable)
				&& GlobalVariable.isTableAvailable(dyToTable))) {
			LOG.error("{} is an unavailable table, please check tables tag");
			System.exit(-1);
		}
						
		// 获取接入数据表的字段列表
		tableConf = tableConfMap.get(toTable);
		fields = tableConf.getFieldConf().getFields();
	}
	
	
	/**
	 * 构建下载的文件的路径与待入库的数据表名的映射关系
	 * @return map<文件路径， 表名> 
	 */
	@Override
	protected Map<String, String> getFileTableMap() {
		//从存储了下载文件列表的文件中读取这些下载的文件列表
		List<String> fileList = getFileList();
		if (null == fileList) {
			LOG.error("get download file items list error in DyFtpImportService:getFileTableMap");
			return null;
		}
		
		//利用文件名构建文件路径与入库表名之间的对应关系
		Map<String, String> pathTableMap = new LinkedHashMap<String, String>();
		Map<String, String> fileTableMap = ftpDownloadConf.getFileTableMap();
		for (String filePath : fileList) {
			File file = new File(filePath);
			String fileName = FileUtil.getShortFileName(file.getName());
			String tableName = fileTableMap.get(fileName);
			if (!CommonUtil.isNullOrEmpty(tableName)) {
				LOG.debug(filePath + "---->" + tableName);
				pathTableMap.put(filePath, tableName);
			}	
		}
		
		//如果fileList不为空，pathTableMap为空
		if (!fileList.isEmpty() && pathTableMap.isEmpty()) {
			LOG.warn("all downloaded file are not meet naming notations");
		}
		return pathTableMap;
	}


	/**
	 * 从存储了文件下载列表项的文件中读取这些下载文件的路径
	 * @return 如果下载失败返回null, 否则返回读取的列表（注意如果是没有文件要下载则返回空列表）
	 * null 指失败   而空指没有文件要下载
	 */
	private List<String> getFileList() {
		//获取记录下载列表的文件路径
		String localRootPath = ftpDownloadConf.getLocalPath();
		if (!localRootPath.endsWith(File.separator)) {
			localRootPath += File.separator;
		}
		//从存储下载列表的文件中读取下载列表
		writeDownloadFilePath = localRootPath + prefix + Common.DOWNLOADFILE;
		LOG.debug("writeDownloadFilePath : " + writeDownloadFilePath);
		List<String> lines = IOUtil.readFile(writeDownloadFilePath);
		if (lines.isEmpty()) {
			LOG.warn("There are no download items of {}!", writeDownloadFilePath);
		} else {
			//为下载列表排序
			Collections.sort(lines, null);
		}
		return lines;
	}
		
	@Override
	protected boolean checkMD5OfFile(String path) {
		if (CommonUtil.isNullOrEmpty(path)) {
			LOG.error("The path which checke md5 is null or empty!");
			return false;
		}
		String md5Suffix = Common.DOT + Common.MD5SUFFIX;
		String dataSuffix = Common.DOT + ftpDownloadConf.getDataFileSuffix();
		if (!path.endsWith(dataSuffix)) {
			LOG.error("suffix format of file {} is error, not data file, please check!", path);
			return false;
		}
		
		String md5File = path.replace(dataSuffix, md5Suffix);
		
		List<String> lines = IOUtil.readFile(md5File);
		if (CommonUtil.isNullOrEmpty(lines)) {
			LOG.error("Content of " + md5File + " is empty");
			return false;
		}
		
		String computeMD5 = null;
		try {
			computeMD5 = MD5Util.getFileMD5String(new File(path));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		LOG.info("compute MD5: " + computeMD5 + "\tfile MD5: " + lines.get(0));
		return computeMD5.equalsIgnoreCase(lines.get(0));
	}

	
	/**
	 * 将从文件中读取的数据构建成对应的sql语句
	 * @param lines 从fileName文件中读取的数据
	 * @param fileName 用于提取该文件相关的字段信息，需要忽略的字段信息
	 * @param dyTableName 待接入的增量数据表名称
	 */
	@Override
	protected List<String> createSql(List<String> lines, String fileName, String dyTableName) {
		//判断参数的正确性
		if (CommonUtil.isNullOrEmpty(lines)) {
			LOG.error("lines is null or empty, please check file {}", fileName);
			return null;
		}
		if (CommonUtil.isNullOrEmpty(fileName) || CommonUtil.isNullOrEmpty(dyTableName)) {
			LOG.error("fileName/tableName is null or empty, file: {},  table: {}", fileName, dyTableName);
			return null;
		}
		LOG.info("There are {} lines in {}", lines.size(), fileName);
		
		//获取文件要忽略（跳过）的字段集合
		HashSet<Integer> skipSet = null;
		if (ftpDownloadConf.getFileSkipMap() != null) {
			skipSet = ftpDownloadConf.getFileSkipMap().get(fileName);
		}
		
		//验证接入数据表的正确性
		this.verifyToTable(dyTableName);
		
		//获取数据表的字段长度映射信息
		fieldLenMap = tableConf.getFieldLenMap();
		if (null == tableConf || CommonUtil.isNullOrEmpty(fields)
				|| null == fieldLenMap) {
			LOG.error("variables is null or empty in method createBigSql");
			return null;
		}
		
		//获取文件中每行数据的字段分隔符
		String separator = jobConf.getFieldSeparator();
		List<String> sqlList = new ArrayList<String>();				
		for (int idx = 0; idx < lines.size(); ++idx) {
			String line = lines.get(idx);
			//根据文件字段的分割符将每行数据分割成字段数组
			String[] rawDatas = line.split(separator);
			if (CommonUtil.isNullOrEmpty(rawDatas)) {
				continue;
			}
			
			//获取该行数据的primarykey,并判断其正确性
			String primaryKeyValue = getPrimaryKey(rawDatas, fileName);
			if (CommonUtil.isNullOrEmpty(primaryKeyValue)) {
				LOG.error("data is {}, not contains primary key!", line);
				continue;
			}
			
			//从原始字段数组中去除要忽略的字段
			String[] datas = null;
			int rawSize = rawDatas.length, rawIdx = 0, newIdx = 0;
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
			
			//根据文件对应数据表的字段信息，拼接SQL语句
			List<String> list = transferDataArrayToSql(datas, fileName, primaryKeyValue);
			if (CommonUtil.isNullOrEmpty(list)) {
				LOG.error("create sql statement error!");
				continue;
			}
			sqlList.addAll(list);			
		}
		return sqlList;
	}
	
	
	/**
	 * @function 保存被正确导入数据库的文件的更新时间与序号
	 * 			 e.g. dir文件夹下201304111050_read_1.txt  
	 * 			 则需要为其保存 dy_read_stamp=201304111050   dy_read_index=1 
	 * @param filePath 代谢如的文件路径
	 */
	@Override
	protected void saveLatestStamp(String filePath) {
		if (CommonUtil.isNullOrEmpty(filePath)) {
			LOG.error("file name is null or empty, cannot save latest stamp for it!");
			return;
		}
		File file = new File(filePath);
		String dir = file.getParentFile().getName();
		String fileName = file.getName();
		String shortName = FileUtil.getShortFileName(fileName);
		LOG.debug("ready to save latest stamp, dir: {}  fileName: {}", dir, fileName);
				
		//保存更新时间
		String stampKey = dir + FileUtil.SEPARATOR + shortName + FileUtil.SEPARATOR + Common.STAMP;
		String stampValue = FileUtil.getTimeFromFileName(fileName);
		Common.saveLastStamp(stampKey, stampValue);
		
		//保存文件序号
		String indexKey = dir + FileUtil.SEPARATOR + shortName + FileUtil.SEPARATOR + Common.INDEX;
		String indexValue = String.valueOf(FileUtil.getIndexFromFileName(fileName));
		Common.saveLastStamp(indexKey, indexValue);
		LOG.debug("save latest stamp for {} success!", filePath);
	}

	/**
	 * @function 获取fileName文件中某一行数据（datas）的主键值
	 * @param datas 代表文件中一行数据的字段值的数组
	 * @param fileName 文件名，即datas就是从这个文件中获取的
	 * @return 主键值
	 */
	private String getPrimaryKey(String[] datas, String fileName) {
		if (CommonUtil.isNullOrEmpty(datas) || CommonUtil.isNullOrEmpty(fileName)) {
			LOG.error("index of primary key of {} is out of boundary!", fileName);
			return null;
		}
		int primaryKeyIdx = ftpDownloadConf.getFilePrimarykeyIdxMap().get(fileName);
		if (primaryKeyIdx > datas.length) {
			LOG.error("Index of primary key of {} is out of boundary! data:{}", fileName, datas.toString());
			return null;
		}
		return datas[primaryKeyIdx - 1];
	}
	
	
	/**
	 * @function 将代表文件中一行数据的字段值数组，根据其中的操作字段值，将数据转化成相应的SQL语句
	 * @param datas 代表一行数据的字段值数组
	 * @param fileName 文件名，datas就来自这个文件
	 * @param primaryKeyValue 本行数据的主键值
	 * @return
	 */
	protected List<String> transferDataArrayToSql(String[] datas, String fileName, String primaryKeyValue) {
		List<String> sqlList = new ArrayList<String>();
		// 检查参数的合法性
		if (CommonUtil.isNullOrEmpty(datas) || CommonUtil.isNullOrEmpty(primaryKeyValue)) {
			return sqlList;
		}
		
		String sql = null;
		StringBuilder sb = null;

		//判断增量文件的第一列operator字段是否合法（应为是0 1 2之一）
		if (!datas[0].matches(Common.REGEXNUMBER)) {
			IOUtil.printArray(datas);
			LOG.error("first line is not an number, so it cannot present an operator!");
			return sqlList;
		}

		
		//获取主键名、本行数据的操作ID
		String primaryKey = tableConf.getPrimaryKey();
		int operator = Integer.valueOf(datas[0]);
		
		//无论哪种操作，都要从全量表中删除
		StringBuilder deleteSb = new StringBuilder(delBaseSql.replace("primaryKey", primaryKey));
		deleteSb.append(primaryKeyValue).append("\"");
		sqlList.add(deleteSb.toString().replace("tableName", this.fullToTable));
		sqlList.add(deleteSb.toString().replace("tableName", this.dyToTable));
		

		//数据的字段数应该比表的字段数多一个op字段
		int fieldSize = Math.min(fields.length, datas.length - 1);
		//是否需要写goodsDelete
		boolean needInsertGoodsDel = true;
		if (Common.ADD == operator) {
			//添加操作不需要写goodsdelete表
			needInsertGoodsDel = false;
			
			//拼接SQL语句中字段
			sb = new StringBuilder(addBaseSql);
			for (int n = 0; n < fieldSize; n++) {
				String key = fields[n];
				String value = datas[n + 1];
				if (!CommonUtil.isNullOrEmpty(value)) {
					sb.append("`" + key + "`,");
				}
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append(") values(");
			
			//拼接SQL语句中
			for (int n = 0; n < fieldSize; n++) {
				String key = fields[n];
				String value = datas[n + 1];
				
				if (CommonUtil.isNullOrEmpty(value)) {
					continue;
				}
				//检查字段长度
				if (!CommonUtil.isNullOrEmpty(fieldLenMap) && fieldLenMap.containsKey(key)) {
					int limitLen = fieldLenMap.get(key);
					if (value.length() > limitLen) {
						value = value.substring(0, fieldLenMap.get(key));
					}
				}
				//拼接SQL values中字段
				sb.append("\"").append(value.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\\"", "\\\\\""))
					.append("\",");
			}
				
			sb.deleteCharAt(sb.length() - 1);
			sb.append(")");
		} else if (Common.UPDATE == operator) {		
			//操作符为更新
			sb = new StringBuilder(updateBaseSql);
			//拼接更新SQL语句中的字段
			for (int idx = 0; idx < fieldSize; ++idx) {
				if (!CommonUtil.isNullOrEmpty(datas[idx + 1])) {
					//检查字段长度
					String key = fields[idx];
					String value = datas[idx + 1];
					if (!CommonUtil.isNullOrEmpty(fieldLenMap) && fieldLenMap.containsKey(key)) {
						int limitLen = fieldLenMap.get(key);
						if (value.length() > limitLen) {
							value = value.substring(0, fieldLenMap.get(key));
						}
					}
					
					sb.append("`").append(key).append("`=\"").append(value
							.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\\"", "\\\\\"")).append("\",");		
//							.replaceAll("\\\\", "").replaceAll("'", "\\'")).append("',");
				}
			}
			sb.deleteCharAt(sb.length() - 1);
			//拼接更新SQL语句的主键值
			sb.append("where ").append(primaryKey).append("='").append(primaryKeyValue).append("'");
		}
		//这里无需处理删除类，因为对于每个ID都添加了从增量、全量数据表删除数据的操作
		
		sql = sb.toString().replace("tableName", this.dyToTable);
		LOG.debug("SQL : " + sql);
		sqlList.add(sql);
		
		//如果是修改或删除类型,则要添加插入到goods_delete的sql语句
		if (needInsertGoodsDel) {
			Map<String, String> tableIndextypeMap = jobConf.getTableIndextypeMap();
			String indexType = tableIndextypeMap.get(fileName);
			String insertSql = insertGoodsDelSql.replace("type_replace", indexType).replace("goodsid_replace", primaryKeyValue);
			LOG.debug("insert goods_delete sql: " + insertSql);
			sqlList.add(insertSql);
		}
		return sqlList;
	}
	
	
//	public static void main(String[] argv) {
//		String table = "dy_read";
//		System.out.println(Common.FULLTABLESUFFIX + table.substring(Common.DYTABLESUFFIX.length()));
//		
//	}
}
