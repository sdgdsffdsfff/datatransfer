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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.sql.CLOB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.conf.DBDownloadConf;
import com.panguso.datatransfer.conf.FieldConf;
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
 * @author liubing
 *
 */
public class DyDBDownloadService extends CommonDownloadService {
	private static final Logger LOG = LoggerFactory.getLogger(DyDBDownloadService.class);
	private final String operateID = "operateid";		//操作ID
	private final String contentID = "contentid";		//内容ID
	private final String operation = "operation";		//操作类型
	
	//从操作表中获取对数据表所做的操作集合SQL
	private final String getOperateSQL = "select operateid, contentid, operation " 
			+ "from operate_table where operateid > lastOperateId order by operateid";
	//从本地数据表中删除数据SQL
	private final String deleteSql = "delete from table_replace where primarykey_replace = \"primarykey_value\"";
	//从源数据表中获取数据
	private final String selectSQL = "select * from table_replace where primarykey_replace = \"primarykey_value\"";
	//增量接入的要删除的ID，插入到goods_delete表的SQL语句
	private final String insertSQL = "insert into goods_delete values('type_replace', 'goodsid_replace')";
	
	
	private JobConf jobConf = null;
	private DBDownloadConf dbConf = null;
	private Map<String, TableConf> tableConfMap = null;
	private Map<String, String> fieldKeyValueMap = null;  //存放映射后的Key与Value的对应
	
//	private String gFromTable = null;
	private FieldConf fromTableFieldConf = null;
	private Map<String, String> fromTablefieldMap = null;
	private String[] fromTablefields = null;
	private Set<String> insertFieldSet = null;				  //存放fromTable映射后的字段列表
	
	//数据要存储到本地的增量和全量数据表，两个表的结构完全一样
//	private String toTable = null;	//这个是不包含增量（full_）、全量（prod_）前缀的表名
	private String fullToTable = null;
	private String dyToTable = null;
	private TableConf toTableConf = null;	
	
	private Set<String> addIDSet = null;	//添加操作ID集合
	private Set<String> delIDSet = null;	//删除操作ID集合
	private Set<String> updateSet = null;	//用这个存储需要写goodsdelete表的ID集合
	private int maxOperateId = 0;	//增量表的最大处理ID

	/**
	 * 	初始化参数变量
	 */
	protected void init() {
		jobConf = GlobalVariable.getJobConf(prefix);
		if (null == jobConf) {
			LOG.error("get JobConf of {} failed in BigDBDownloadService", prefix);
			System.exit(-1);
		}
		
		dbConf = jobConf.getDbConf();
		if (null == dbConf) {
			LOG.error("get DBDownloadConf of {} failed in BigDBDownloadService", prefix);
			System.exit(-1);
		}
		
		tableConfMap = GlobalVariable.getTableConfMap();
		if (CommonUtil.isNullOrEmpty(tableConfMap)) {
			LOG.error("table config map is null or empty, please check the tables tag of config.xml!");
			System.exit(-1);
		}
		
		String toTableName = dbConf.getToTable();
		//这里要验证full_toTable 和  prod_toTable
		if (!verifyToTable(toTableName)) {
			LOG.error("{} is unavailable, please check the tables tag of config.xml!", toTableName);
			System.exit(-1);
		}
		
		toTableConf = tableConfMap.get(dyToTable);
		
		//insertFieldSet = 来源表中所有字段  去除跳过字段（skip字段），更新映射字段(map字段) 
		insertFieldSet = new HashSet<String>();
		fieldKeyValueMap = new HashMap<String, String>();
	}
	

	/**
	 * 验证增量入库表和全量入库表的可用性
	 * @param toTable 待验证的表名（必须配置为全量表名）
	 * @return 验证结果
	 */
	private boolean verifyToTable(String toTable) {
		if (CommonUtil.isNullOrEmpty(toTable)) {
			return false;
		}
		//验证数据表名的正确性，DB的增量接入配置的增量表名
		if (!toTable.startsWith(Common.DYTABLESUFFIX)) {
			LOG.error("table name is not started with dy_, can not import data into db!");
			System.exit(-1);
		}
		
		//获取本job的接入增量、全量表
		this.dyToTable = toTable;
		this.fullToTable = Common.FULLTABLESUFFIX + toTable.substring(Common.DYTABLESUFFIX.length());
		
		//获取并验证全赖那个数据表
//		String fullTable = Common.FULLTABLESUFFIX + toTable;
//		if (!tableConfMap.containsKey(fullTable)) {
//			LOG.error("{} is unavailable, not found in tableConfMap, check to_table and tables tag of config.xml!", fullTable);
//			return false;
//		}
		
		
		//获取并验证增量数据表
//		String dyTable = Common.DYTABLESUFFIX + toTable;
//		if (!tableConfMap.containsKey(dyTable)) {
//			LOG.error("{} is unavailable, not found in tableConfMap, check to_table and tables tag of config.xml!", dyTable);
//			return false;
//		}
		
		if (!(GlobalVariable.isTableAvailable(dyToTable)
				&& GlobalVariable.isTableAvailable(fullToTable))) {
			LOG.error("{} is an unavailable table, please check tables tag");
			System.exit(-1);
		}
		return true;
	}
	
	
	/**
	 * 初始化各个ID集合
	 * addIDSet：增加和修改数据，都会写入增量数据表中
	 * delIDSet：无论哪种操作，都要先删除增量、全量数据表中有关该ID的旧数据
	 * updateSet：对于修改和删除的ID，要写入goodsdelete表中
	 */
	private void initIDSet() {
		//初始化添加集合
		if (null == addIDSet) {
			addIDSet = new HashSet<String>();
		}
		addIDSet.clear();
		
		//初始化删除集合
		if (null == delIDSet) {
			delIDSet = new HashSet<String>();
		}
		delIDSet.clear();
		
		//初始化修改和删除的集合
		if (null == updateSet) {
			updateSet = new HashSet<String>();
		}
		updateSet.clear();
	}
		
	/**
	 * 获取从来源表获取对应的操作表名称，并判断该表是否存在
	 * @param table 数据来源表，即from表
	 * @return
	 */
	private String getOperateTable(String table) {
		if (CommonUtil.isNullOrEmpty(table)) {
			LOG.error("table name in null or empty， so cannot get operate table");
			return null;
		}
		//拼接操作表的表名，进行验证
		String operateTable = Common.OPERATIONPREFIX + table;
		if (!GlobalVariable.isTableAvailable(operateTable)) {
			operateTable = null;
			LOG.error("operate table {} of {} is unavailable, please check tables tag in config.xml", operateTable, table);
		} else {
			LOG.debug("operate table for {} is : {}", table, operateTable);
		}
		return operateTable;
	}
	

	/**
	 * 从property文件中读取上次对来源表增量处理更新的操作ID
	 * @param table 数据来源表表名
	 * @return	最大操作ID
	 */
	private String getLastOperateId(String table) {
		String key = jobConf.getJobName() + FileUtil.SEPARATOR + table + Common.LASTOPIDSUFFIX;
		LOG.debug("get last operate id tag is: {}", key);
		String lastOperateIdStr = Common.getLastOperateID(key);
		
		//如果获取的最大操作ID格式不对，设置成默认值0
		if (!lastOperateIdStr.trim().matches(Common.REGEXINTEGER)) {
			lastOperateIdStr = "0";
		}		
		return lastOperateIdStr;
	}
	

	@Override
	public boolean doService() {
		//初始化各参数
		init();
		
		//判断数据来源表（from table）是否是有效的（即是配置文件的tables中的一项）
		List<String> fromTableList = dbConf.getFromTableList();
		if (!GlobalVariable.isAllTablesAvailable(fromTableList)) {
			LOG.error("some from table is unavailable, please check");
			return false;
		}
		
		//遍历处理每个数据来源表
		for (String fromTable : fromTableList) {
			if (!initFromTableInfo(fromTable)) {
				LOG.error("init {}'s information error!", fromTable);
				continue;
			}
			
			//获取来源表对应的操作表名称
			String operateTable = getOperateTable(fromTable);
			if (null == operateTable) {
				LOG.error("cannot get an available operate table for {}", fromTable);
				continue;
			}
			
			//获取来源表保存的上次最后更新的操作ID
			String key = jobConf.getJobName() + FileUtil.SEPARATOR + fromTable + Common.LASTOPIDSUFFIX;
			String lastOperateId = getLastOperateId(fromTable);
			//获取操作ID集合的SQL语句
			String getOperateSql = getOperateSQL.replace("operate_table", operateTable)
										.replace("lastOperateId", lastOperateId);
			LOG.debug("operate sql is: {}", getOperateSql);
			
			//数据库连接参数定义
			Connection conn = null;			//连接
			PreparedStatement pstmt = null;	
			ResultSet operateIDRst = null;			//结果集
			long startTime = System.currentTimeMillis();
			try {
				//数据库连接参数初始化
				conn = SQLUtil.getConnection(tableConfMap.get(operateTable).getDs()); 
				pstmt = conn.prepareStatement(getOperateSql);
				
				//连接操作表，获取从上次完成时间到现在的所有操作ID
				//增量数据的量比较小，所以采用一次读取的方式进行读取
				operateIDRst = SQLUtil.getResultSet(pstmt);
				if (null == operateIDRst) {
					LOG.error("get result set from {} error!", operateTable);
					continue;
				}
				
				//对操作ID集合进行分类
				int operateSetSize = classifyOperateIDSet(operateIDRst);
				if (operateSetSize < 0) {
					LOG.error("classfy operate id set error!");
					continue;
				} else if (operateSetSize == 0) {
					LOG.info("operate id set is empty!");
					continue;
				}
				LOG.info("get operate id set success!");
				
				//处理操作集合中的删除集合
				if (!handleDeleteIdSet()) {
					LOG.error("delete operate id set error!");
					continue;
				}
				
				//处理要添加或更新的操作ID集合
				if (!handleAddOrUpdateIDSet(fromTable)) {
					LOG.error("add operate id set error!");
					continue;
				}
				//保存本次执行的最大操作ID
				LOG.info("The max operate ID is : {}", this.maxOperateId);
				Common.saveLastStamp(key, String.valueOf(maxOperateId));
			} catch (SQLException e) {
				LOG.error("when getting data from {} error: {}", operateTable, e.toString());
				return false;
			} finally {
				SQLUtil.close(operateIDRst);
				SQLUtil.close(operateIDRst);
				SQLUtil.close(conn);
			}
			long endTime = System.currentTimeMillis();
			LOG.info("update operate id for {} success! cost {}", fromTable, DateUtil.getReadableTime(endTime - startTime));
		}
		//接入完成后，创建done文件
		FileUtil.createDoneFile(jobConf);
		return true;
	}

	
	/**
	 * 对操作ID集合进行分类（分为添加、修改、删除三个集合）
	 * @param 从数据库中获取的ID结果集
	 * @return 集合内包含的ID总量
	 */
	private int classifyOperateIDSet(ResultSet rst) {
		if (null == rst) {
			LOG.error("ResultSet need to handle is null");
			return -1;
		}

		//清空并初始化ID集合
		initIDSet();

		int total = 0;
		try {
			while (rst.next()) {
				//获取内容ID，操作类型，和操作ID
				int operateid = rst.getInt(operateID);		
				String contentid = rst.getString(contentID);	//对应数据表的主键
				int operator = rst.getInt(operation);
				maxOperateId = operateid;
				//在添加或修改前也都需要先删除
				delIDSet.add(contentid);
				
				switch (operator) {
				case Common.ADD:
					//添加操作不需要写入goodsdelete
					addIDSet.add(contentid);
					break;
				case Common.UPDATE:
					addIDSet.add(contentid);
					updateSet.add(contentid);
					break;
				case Common.DELETE:
					addIDSet.remove(contentid);
					updateSet.add(contentid);
					break;
				default:
					break;
				}
				total++;
			}			
		} catch (SQLException e) {
			LOG.error("handle ResultSet of operate ID error!");
			e.printStackTrace();
			return -1;
		}
		LOG.debug("delete set size: " + delIDSet.size());
		LOG.debug("add set size: " + addIDSet.size());
		LOG.debug("update set size: " + updateSet.size());
		return total;
	}
	

	/**
	 * 对于修改、删除的数据都要添加的goodsdelete数据表中
	 * @return 待插入goodsdelete表的SQL语句
	 */
	private List<String> getInsertGoodsdeleteSqlList() {
		List<String> insertGoodsDelList = new ArrayList<String>();
		
		if (CommonUtil.isNullOrEmpty(updateSet)) {
			LOG.info("no data need to write into goodsdelete");
			return insertGoodsDelList;
		}
		
		for (String contentid : updateSet) {	
			// 获取数据文件对应的索引类型 
			Map<String, String> tableIndextypeMap = jobConf.getTableIndextypeMap();
			String indexType = tableIndextypeMap.get(dyToTable);
			if (null == indexType) {
				LOG.error("get indexType of {} error, check table_indextype_map of {}", dyToTable, prefix);
			}
			String insertSql = insertSQL.replace("type_replace", indexType).replace("goodsid_replace", contentid);
			LOG.debug("insert goods_delete sql: " + insertSql);
			insertGoodsDelList.add(insertSql);
		}
		
		return insertGoodsDelList;
	}
	
	/**
	 * 处理删除ID集合
	 * @return 处理结果
	 */
	private boolean handleDeleteIdSet() {
		//获取表配置，增量和全量表的配置一样，除了名字
		String primaryKey = toTableConf.getPrimaryKey();
		LOG.debug("primary key of {} is {}", dyToTable, primaryKey);		
		
		//删除增量表和全量表中的主键为contentid的数据
		String delSql = deleteSql.replace("primarykey_replace", primaryKey); 
		List<String> allDelSqlList = new ArrayList<String>();
		for (String contentid : delIDSet) {
			String sql = delSql.replace("table_replace", fullToTable).replace("primarykey_value", contentid);
			LOG.debug("delete sql: " + sql);
			allDelSqlList.add(sql);
			sql = sql.replace(fullToTable, dyToTable);
			LOG.debug("delete sql: " + sql);
			allDelSqlList.add(sql);
		}
		//将插入goodsdelete的SQL语句集合添加到待执行的SQL语句集合中
		allDelSqlList.addAll(getInsertGoodsdeleteSqlList());
		
		//这里默认 goods_delete表和fullToTable在同一个数据库中
		return SQLUtil.executeSql(allDelSqlList, tableConfMap.get(fullToTable).getDs());
	}
	

	/**
	 * 处理要添加或修改的ID集合
	 * @param fromTable 数据来源表，因为需要到这个表里去取最新数据接入更新
	 * @return 处理结果
	 */
	private boolean handleAddOrUpdateIDSet(String fromTable) {
		if (CommonUtil.isNullOrEmpty(fromTable)) {
			LOG.error("date table is null or empty!(in handleOperateIdSet method)");
			return false;
		}
		
		//用来记录执行失败的信息
		List<String> errorList = new ArrayList<String>();
				
		//增量接入表相关的数据库相关参数
		int total = 0;
		Connection toConn = null;
		PreparedStatement toPstmt = null;
		
		//与数据来源表有关的数据库连接等参数
		Connection fromConn = null;
		PreparedStatement fromPstmt = null;
		ResultSet rst = null;
		long startTime = System.currentTimeMillis();
		try {
			//设置与数据来源表的连接
			TableConf tableConf = tableConfMap.get(fromTable);
			fromConn = SQLUtil.getConnection(tableConf.getDs());
			String primaryKey = tableConf.getPrimaryKey();

			//初始化增量接入表的数据库连接参数
			TableConf dyToTableConf = tableConfMap.get(dyToTable);
			toConn = SQLUtil.getConnection(dyToTableConf.getDs());

			//关闭事务自动提交
			toConn.setAutoCommit(false);	

			//初始化插入数据到增量数据表的基础SQL语句
			String insertSql = SQLUtil.getInsertSql(dyToTable, insertFieldSet); 
			LOG.debug("base insert SQL : {}", insertSql);
			if (null == insertSql) {
				return false;
			}
			toPstmt = toConn.prepareStatement(insertSql);
			
			//替换查询SQL语句中的表名与主键
			String selectSql = selectSQL.replace("table_replace", fromTable)
								.replace("primarykey_replace", primaryKey);
			//获得增量接入表的字段配置信息
			FieldConf toTableFieldConf  = dyToTableConf.getFieldConf();
			for (String contentid : addIDSet) {
				String sql = selectSql.replace("primarykey_value", contentid);
				LOG.debug("select sql: {}", sql);
				//从数据来源表中获取主键为contentid的数据集
				fromPstmt = fromConn.prepareStatement(sql);
				rst = SQLUtil.getResultSet(fromPstmt);				
				if (null == rst) {
					String err = "exec" + sql + " error!";
					errorList.add(err);
					continue;
				}
				
				while (rst.next()) {
					//获取数据集rst中获取各字段对应的值
					getKeyValueOfResultSet(rst);
				}
				
				//根据字段在要插入的数据表（toTable）中的类型，更新SQL插入语句
				if (!setPreparedStatement(toPstmt, toTableFieldConf)) {
					String err = "create sql for " + contentid + " error!";
					errorList.add(err);
					continue;
				}
				
				//添加到SQL命令列表
				toPstmt.addBatch();
				++total;
			}
			
			//执行批量更新，提交事务
			try {
				toPstmt.executeBatch();
				toConn.commit();
			} catch (BatchUpdateException e) {
				LOG.error("{}", e.toString());
				List<String> errSqlList = new ArrayList<String>();
				errSqlList.add("DyDBDownloadService:handleAddOrUpdateIdSet() exec " + insertSql + " failed: " + e.toString());
				IOUtil.writeToCoreFile(errSqlList);
				try {
					toConn.commit();
				} catch (Exception e2) {
					LOG.error("{}", e2);
				}
			}
			
			long endTime = System.currentTimeMillis();
			LOG.info("insert data into " + dyToTable + " cost " + DateUtil.getReadableTime(endTime - startTime));
			LOG.info("There are total " + total + " lines data");
		} catch (SQLException e) {
			LOG.error("put data into DB error!");
			e.printStackTrace();
			return false;
		} finally {
			SQLUtil.close(rst);
			SQLUtil.close(toPstmt);
			SQLUtil.close(toConn);
		}
		
		if (!errorList.isEmpty()) {
			IOUtil.writeToCoreFile(errorList);
		}
		return true;
	}
	
	//设置PreparedStatement	
	private boolean setPreparedStatement(PreparedStatement pstmt, FieldConf fieldConf) {
		try {
			int index = 1;
			for (String field : insertFieldSet) {				
				String valueStr = fieldKeyValueMap.get(field);
				if (fieldConf.getIntset().contains(field)) {
					int value = 0;
					if (valueStr != null) {
						value = Integer.parseInt(valueStr);
					}
					pstmt.setInt(index, value);
				} else if (fieldConf.getLongset().contains(field)) {
					long value = 0L;
					if (valueStr != null) {
						value = Long.parseLong(valueStr);
					}
					pstmt.setLong(index, value);
				} else if (fieldConf.getFloatset().contains(field)) {
					float value = 0f;
					if (valueStr != null) {
						value = Float.parseFloat(valueStr);
					}
					pstmt.setFloat(index, value);
				} else if (fieldConf.getDatetimeset().contains(field)) {
					Timestamp timeStamp = new Timestamp(0L);
					if (valueStr != null) {
						timeStamp = new Timestamp(Long.parseLong(valueStr));
					}
					pstmt.setTimestamp(index, timeStamp);
				} else if (fieldConf.getTimeset().contains(field)) {
					Time time = null;
					if (valueStr != null) {
						time = new Time(Long.parseLong(valueStr));
					}
					pstmt.setTime(index, time);
				} else if (fieldConf.getTimestampset().contains(field)) {
					Timestamp timeStamp = null;
					if (valueStr != null) {
						timeStamp = new Timestamp(Long.parseLong(valueStr));
					}
					pstmt.setTimestamp(index, timeStamp);
				} else {
					pstmt.setString(index, valueStr);
				}
				++index;
			}
		} catch (SQLException e) {
			LOG.error("setPreparedStatement error!");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	

	/**
	 * 初始化数据来源表相关信息
	 * @param fromTable 待初始化的数据来源表表名
	 * @return 初始化结果
	 */
	private boolean initFromTableInfo(String fromTable) {
		
//		gFromTable = fromTable;
		//获取数据来源表（fromTable）的字段等信息
		fromTableFieldConf = tableConfMap.get(fromTable).getFieldConf();
		fromTablefieldMap = dbConf.getTableMap().get(fromTable);
		fromTablefields = fromTableFieldConf.getFields();
		
		//处理数据来源表（fromTable）的忽略字段和映射字段
		if (!getValidFieldSet(fromTable)) {
			LOG.error("get insertFieldSet error!");
			return false;
		}
		return true;
	}
	

	/**
	 * 从数据库结果集中提取key-value键值对放入全局变量
	 * @param rst 待抽取的数据库结果集
	 */
	private void getKeyValueOfResultSet(ResultSet rst) {	
		
		try {
			for (String field : fromTablefields) {
				//获取映射名称（如果存在的话）
				String fieldMappedName = field;  //映射后的field名称
				String fieldValue = null;
				
				if (fromTablefieldMap.containsKey(field)) {
					fieldMappedName = fromTablefieldMap.get(field);
				}
				
				//根据该字段的实际类型获取该字段的值
				if (fromTableFieldConf.getIntset().contains(field)) {
					fieldValue = String.valueOf(rst.getInt(field));
				} else if (fromTableFieldConf.getLongset().contains(field)) {
					fieldValue = String.valueOf(rst.getLong(field));
				} else if (fromTableFieldConf.getFloatset().contains(field)) {
					fieldValue = String.valueOf(rst.getFloat(field));
				} else if (fromTableFieldConf.getDatetimeset().contains(field)) {
			          Date date = rst.getTimestamp(field);
			          if (date != null) {
			        	  fieldValue = String.valueOf(date.getTime());
			          }
			    } else if (fromTableFieldConf.getTimeset().contains(field)) {
			          Time time = rst.getTime(field);
			          if (time != null) {
			        	  fieldValue = String.valueOf(time.getTime());
			          }
			    } else if (fromTableFieldConf.getTimestampset().contains(field)) {
			          Timestamp timeStamp = rst.getTimestamp(field);
			          if (timeStamp != null) {
			        	  fieldValue = String.valueOf(timeStamp.getTime());
			          }
			    } else if (fromTableFieldConf.getClobset().contains(field)) {
			          CLOB clob = (CLOB) rst.getClob(field);
			          if (clob != null) {
			        	  fieldValue = SQLUtil.clob2String(clob);
			              LOG.debug("clob content:" + fieldValue);
			          }
			    } else {
			    	fieldValue = rst.getString(field);
			    }
	
				fieldKeyValueMap.put(fieldMappedName, fieldValue);
			}
		} catch (SQLException e) {
			LOG.error("get value from ResultSet error!{}", e);
		}
	}
	

	/**
	 * 对来源表的字段进行过滤，名称映射后的接入字段集合
	 * @param fromTable 待处理的数据来源表
	 * @return
	 */
	private boolean getValidFieldSet(String fromTable) {
//		//获取来源表的字段数组
//		String[] fields = fromTableFieldConf.getFields();
//		
//		if (CommonUtil.isNullOrEmpty(fields)) {
//			LOG.error("{} is an unavailable table, There are no fields of it!", fromTable);
//			return false;
//		}
		
		//从DBConf（或FTPConf）中获取skip和字段映射表，因为针对不同的		
//		Set<Integer> fieldSkipSet = dbConf.getTableSkip().get(fromTable);
		
//		//过滤掉忽略字段
//		if (!CommonUtil.isNullOrEmpty(fieldSkipSet)) {
//			for (int idx = 1; idx <= fields.length; ++idx) {
//				if (!fieldSkipSet.contains(idx)) {
//					String field = fields[idx - 1];
//					insertFieldSet.add(field);
//				}
//			}
//		} else {
//			for (String field : fields) {
//				insertFieldSet.add(field);
//			}
//		}
		
		//处理数据提供方的表与我方表字段的映射关系
//		Map<String, String> fromTablefieldMap = dbConf.getTableMap().get(fromTable);
		if (null == fromTablefieldMap) {
			LOG.error("fromTablefieldMap is null in DyDBDownloadService:getValidFieldSet, please check!");
			System.exit(-1);
		}

		//针对fromTable中每个field，查看是否有需要映射的
		for (String field : fromTablefields) {
			if (fromTablefieldMap.containsKey(field)) {
				field = fromTablefieldMap.get(field);
			}
			insertFieldSet.add(field);
		}

			
//		
//		if (!CommonUtil.isNullOrEmpty(fromTablefieldMap)) {
//			Set<String> fieldSet = new HashSet<String>();
//			//针对fromTable中每个field，查看是否有需要映射的
//			for (String field : insertFieldSet) {
//				if (fromTablefieldMap.containsKey(field)) {
//					field = fromTablefieldMap.get(field);
//				}
//				fieldSet.add(field);
//			}
//			insertFieldSet = fieldSet;
//		}
		
		
		
		if (insertFieldSet.isEmpty()) {
			LOG.error("There are no fields left after mapping fields");
			return false;
		}
		
//		//验证这些字段是否是toTable字段的子集
//		String[] toTableFields = toTableConf.getFieldConf().getFields();
//		Set<String> toTableFieldSet = CommonUtil.getSetFromArray(toTableFields);
//		if (!toTableFieldSet.containsAll(insertFieldSet)) {
//			LOG.error("mapped field of from table is not all available, please check from_table field map");
//			insertFieldSet = null;
//			return false;
//		}
		return true;
	}
}

