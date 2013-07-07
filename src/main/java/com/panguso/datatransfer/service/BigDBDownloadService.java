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
import java.sql.Statement;
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
import com.panguso.datatransfer.config.ConfigFactory;
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
public class BigDBDownloadService extends CommonDownloadService {
	private static final Logger LOG = LoggerFactory.getLogger(BigDBDownloadService.class);

	private JobConf jobConf = null;						  //job配置相关信息
	private DBDownloadConf dbConf = null;				  //要下载的数据库相关信息
	private Map<String, TableConf> tableConfMap = null;	  //与本次下载有关的数据表的信息
	private Map<String, String> fieldKeyValueMap = null;  //存放从数据库中取的数据集的Key与Value的对应
	
	private TableConf fromTableConf = null;				  //数据来源表相关信息
	private FieldConf fromTableFieldConf = null;		  //数据来源表的字段信息
	private Map<String, String> fromTablefieldMap = null;
	private String[] fromTablefields = null;
	private Set<String> insertFieldSet = null;				  //存放fromTable映射后的字段列表
	
	private String toTable = null;
	private TableConf toTableConf = null;
	
	/**
	 * 初始化参数变量
	 */
	protected void init() {
		//初始化prefix对应的job相关配置JobConf
		jobConf = GlobalVariable.getJobConf(prefix);
		if (null == jobConf) {
			LOG.error("get jobConf from GlobalVariable failed in BigDBDownloadService");
			System.exit(-1);
		}
		//初始化prefix对应的数据库相关配置DBConf
		dbConf = jobConf.getDbConf();
		if (null == dbConf) {
			LOG.error("get dbConf failed in BigDBDownloadService");
			System.exit(-1);
		}
		//初始化全局数据表相关配置的map
		tableConfMap = GlobalVariable.getTableConfMap();
		if (CommonUtil.isNullOrEmpty(tableConfMap)) {
			LOG.error("table config map is null or empty, please check the tables tag of config.xml!");
			System.exit(-1);
		}
		//默认一个接入job只有一个接入表
		toTable = dbConf.getToTable();
		
//		if (CommonUtil.isNullOrEmpty(toTable)) {
//			LOG.error("toTable is null or empty, please check the tables tag of config.xml!");
//			System.exit(-1);
//		}
//		
//		if (!tableConfMap.containsKey(toTable)) {
		if (!GlobalVariable.isTableAvailable(toTable)) {
			LOG.error("toTable is unavailable, please check to_table tag of config.xml!");
			System.exit(-1);
		}
		toTableConf = tableConfMap.get(toTable);
		
		//insertFieldSet = 来源表中所有字段  去除跳过字段（skip字段），更新映射字段(map字段) 
		insertFieldSet = new HashSet<String>();
		fieldKeyValueMap = new HashMap<String, String>();
	}
	
//	//检查from table 是否全都可用，即都在tables配置文件中
//	private boolean isAllFromTablesAvailable(List<String> tableList) {
//		if (CommonUtil.isNullOrEmpty(tableList)) {
//			LOG.error("from table list is null or empty, please check!");
//			return false;
//		}
//
//		boolean allAvailable = true;		
//		for (String table : tableList) {
//			if (!tableConfMap.containsKey(table)) {
//				LOG.error(table + " is not available!");
//				allAvailable = false;
//				break;
//			}
//		}
//		return allAvailable;
//	}
	
	
	private boolean initFromTableInfo(String fromTable) {
		//获取数据来源表（fromTable）的字段等信息
		fromTableConf = tableConfMap.get(fromTable);
		fromTableFieldConf = fromTableConf.getFieldConf();
		fromTablefieldMap = dbConf.getTableMap().get(fromTable);
		fromTablefields = fromTableFieldConf.getFields();
		
		//处理数据来源表（fromTable）的映射字段
		if (!getValidFieldSet(fromTable)) {
			LOG.error("get insertFieldSet error!");
			return false;
		}
		return true;
	}
	
	
	@Override
	public boolean doService() {
		//初始化
		init();
		
		//判断数据来源表（from table）是否是有效的（即是配置文件的tables中的一项）
		List<String> fromTableList = dbConf.getFromTableList();
		if (!GlobalVariable.isAllTablesAvailable(fromTableList)) {
			LOG.error("some from table is unavailable, please check");
			return false;
		}
		
		//每次处理的数据条数
		int limit = ConfigFactory.getInt(Common.SQLLIMIT, Common.DEFAULTLIMIT);
		String baseSql = "select * from table_replace limit start_replace, end_replace";
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		Connection conn = null;
		
		//在入库之前，先清空接入数据表
		if (cleanToTable()) {
			LOG.info("delete data from {} success!", this.toTable);
		}
		
		for (String fromTable : fromTableList) {
			if (!initFromTableInfo(fromTable)) {
				LOG.error("init {} information error!", fromTable);
				continue;
			}
			//连接数据库，每次取limit条数据，进行处理
			try {
				int start = 0, end = 0;
				boolean hasMoreData = true;
			
				conn = SQLUtil.getConnection(fromTableConf.getDs());
				String selectSql = baseSql.replace("table_replace", fromTable);
				while (hasMoreData) {
					//更新本次要查询结果集的起始结束
					start = end;
					end = end + limit;
					hasMoreData = false;
					//更新查询SQL语句
					String sql = selectSql.replace("start_replace", String.valueOf(start))
									.replace("end_replace", String.valueOf(limit));
					LOG.debug("SQL: {}", sql);
					//获取结果集
					pstmt = conn.prepareStatement(sql);
					rst = SQLUtil.getResultSet(pstmt);
					if (null == rst) {
						LOG.error("exec {} error!", sql);
						continue;
					}
					
					//将本次取的数据集插入到要入库的数据表中（to table）
					if (putDataIntoDB(fromTable, rst)) {
						hasMoreData = true;
					}
					SQLUtil.close(rst);
				}
			} catch (Exception e) {
				LOG.error("get data from {} error, {}", fromTable, e);
				return false;
			} finally {
				SQLUtil.close(rst);
				SQLUtil.close(pstmt);
				SQLUtil.close(conn);				
			}
			LOG.info("transfer data of {} success!", fromTable);
		}
		
		//接入完成后，创建done文件
		FileUtil.createDoneFile(jobConf);
		
		return true;
	}

	//将来自fromTable数据表中的数据集rst插入到totable中去
	private boolean putDataIntoDB(String fromTable, ResultSet rst) {
		if (null == rst || CommonUtil.isNullOrEmpty(fromTable)) {
			LOG.error("ResultSet or fromTable is null or empty, please check!");
			return false;
		}
			
		String insertSql = SQLUtil.getInsertSql(toTable, insertFieldSet); 
		LOG.debug("baseSQL : " + insertSql);
		if (CommonUtil.isNullOrEmpty(insertSql)) {
			LOG.error("get base SQL error, please check to_table and fields of from_table");
			return false;
		}
		
		long startTime = System.currentTimeMillis();
		//用批处理方式执行多条插入语句
		int total = 0, errTotal = 0, rightTotal = 0;
		try {
			PreparedStatement pstmt = null;
			Connection conn = SQLUtil.getConnection(toTableConf.getDs());
			conn.setAutoCommit(false);	//关闭事务自动提交
			pstmt = conn.prepareStatement(insertSql);
			
			FieldConf toTableFieldConf  = toTableConf.getFieldConf();
			while (rst.next()) {
				//获取数据集rst中获取各字段对应的值
				getKeyValueOfResultSet(rst);
				
				//根据字段在要插入的数据表（toTable）中的类型，更新SQL插入语句
				if (!setPreparedStatement(pstmt, toTableFieldConf)) {
					LOG.error("set preparedStatement failed!");
					continue;
				}
				//添加到SQL命令列表
				pstmt.addBatch();
				++total;
			}
			//执行批量更新，提交事务
			
			try {
				pstmt.executeBatch();
				conn.commit();
			} catch (BatchUpdateException e) {
				LOG.error("{}", e.toString());
				int[] retcodes = e.getUpdateCounts();
				for (int retcode : retcodes) {
					if (retcode == Statement.EXECUTE_FAILED) {
						++errTotal;
					}
				}
				
				List<String> errSqlList = new ArrayList<String>();
				errSqlList.add("exec " + insertSql + " failed: " + e.toString());
				IOUtil.writeToCoreFile(errSqlList);
				try {
					conn.commit();
				} catch (Exception e2) {
					LOG.error("{}", e2);
				}
			}
			
			rightTotal = total - errTotal;
			long endTime = System.currentTimeMillis();
			LOG.info("There are total " + rightTotal + " datas, insert into DB cost " 
						+ DateUtil.getReadableTime(endTime - startTime));
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error("put data into DB error!");
		}
		return (rightTotal > 0);
	}

	
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
	 * 对来源表的字段进行名称映射后的接入字段集合
	 * @param fromTable 待处理的数据来源表
	 * @return
	 */
	private boolean getValidFieldSet(String fromTable) {
		//处理数据提供方的表与我方表字段的映射关系
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

		if (insertFieldSet.isEmpty()) {
			LOG.error("There are no fields left after mapping fields");
			return false;
		}
		return true;
	}
	
	
//	//从数据来源表的字段集合中去除过滤字段，修改映射字段，得到实际要插入数据库的字段
//	private boolean getInsertFieldSet(String fromTable) {
//		//检查来源表的字段列表是否存在
//		if (CommonUtil.isNullOrEmpty(fromTablefields)) {
//			LOG.error("There are no fields in " + fromTable);
//			return false;
//		}
//		//获取在job中配置的关于本表的忽略字段集合
////		Set<Integer> fieldSkipSet = dbConf.getTableSkip().get(fromTable);
////		
////		//过滤掉忽略字段
////		if (!CommonUtil.isNullOrEmpty(fieldSkipSet)) {
////			for (int idx = 1; idx <= fromTablefields.length; ++idx) {
////				if (!fieldSkipSet.contains(idx)) {
////					String field = fromTablefields[idx - 1];
////					insertFieldSet.add(field);
////				}
////			}
////		} else {
////			for (String field : fromTablefields) {
////				insertFieldSet.add(field);
////			}
////		}
//		
//		//处理映射字段
//		if (!CommonUtil.isNullOrEmpty(fromTablefieldMap)) {
//			Set<String> fieldSet = new HashSet<String>();
//			for (String field : insertFieldSet) {
//				if (fromTablefieldMap.containsKey(field)) {
//					field = fromTablefieldMap.get(field);
//				}
//				fieldSet.add(field);
//			}
//			insertFieldSet = fieldSet;
//		}
//		
//		if (insertFieldSet.isEmpty()) {
//			LOG.error("There are no fields left after removing filter fields");
//			return false;
//		}
//		
//		//验证这些字段是否是toTable字段的子集
//		String[] toTableFields = toTableConf.getFieldConf().getFields();
//		Set<String> toTableFieldSet = CommonUtil.getSetFromArray(toTableFields);
//		if (!toTableFieldSet.containsAll(insertFieldSet)) {
//			LOG.error("mapped field of from table is not all available, please check from_table field map");
//			insertFieldSet = null;
//			return false;
//		}
//		return true;
//	}

	/**
	 * 数据接入之前先清空全量数据表
	 * @return
	 */
	private boolean cleanToTable() {
		if (CommonUtil.isNullOrEmpty(this.toTable)) {
			LOG.error("to table is empty or null, can not delete data of it");
		}
		String delSql = "delete from " + this.toTable;
		List<String> sqlList = new ArrayList<String>();
		sqlList.add(delSql);
		return SQLUtil.executeSql(sqlList, tableConfMap.get(toTable).getDs());
	}
}
