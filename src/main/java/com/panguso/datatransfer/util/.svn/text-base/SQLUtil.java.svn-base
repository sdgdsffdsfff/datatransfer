/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import oracle.sql.CLOB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;



/**
 * @author liubing
 *
 */
public final class SQLUtil {
	private static final Logger LOG = LoggerFactory
			.getLogger(SQLUtil.class);	
	private static HashMap<String, DSinfo> dsmap = null;
	//最大尝试次数 
	private static final int MAXTRY = ConfigFactory.getInt(Common.MAXTRY, Common.DEFAULTMAXTRY); 
	
	/**
	 * @author liubing
	 *
	 */
	private static class DSinfo {
		private String name;
		private String driver;
		private String url;
		private String username;
		private String password;
		private int timeout;
	}
	
	/**
	 * 构造函数
	 */
	private SQLUtil() {
		
	}
	
	
	public static HashMap<String, DSinfo> getDsmap() {
		return dsmap;
	}


	public static void setDsmap(HashMap<String, DSinfo> dsmap) {
		SQLUtil.dsmap = dsmap;
	}

	/**
	 * @param offset 本次的起始位置
	 * @param ncount 要取的个数
	 * @param sql 原始SQL
	 * @return
	 */
	public static String getBatchSQL(int offset, int ncount, String sql) {
		return sql + " limit " + offset + " ," + ncount;
	}
	
	
    /**
     * @return 数据库连接
     */
    public static Connection getConnection() {
    	Connection conn = null;
    	String url = ConfigFactory.getString("jdbc.url");
    	String username = ConfigFactory.getString("jdbc.username");
    	String password = ConfigFactory.getString("jdbc.password");
    	int timeout = ConfigFactory.getInt("jdbc.timeout", 10);
    	try {
	    	Class.forName("com.mysql.jdbc.Driver");  
	    	DriverManager.setLoginTimeout(timeout);
	        conn = DriverManager.getConnection(url, username, password);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	return conn;
    }
    
    
    /**
     * @function 根据数据库连接对象获取数据库连接
     * @param info 数据库连接对象
     * @return 数据库连接
     */
    public static Connection getConnection(DSinfo info) {
    	Connection conn = null;
    	
    	int tried = 0;
    	int maxTry = ConfigFactory.getInt(Common.MAXTRY, Common.DEFAULTMAXTRY);
    	long sleepTime = (long) ConfigFactory.getInt(Common.SLEEP, (int) Common.DEFAULTSLEEPTIME);
    	
    	while (conn == null && tried < maxTry) {
        	try {
        		String url = info.url;
            	String username = info.username;
            	String password = info.password;
    	    	DriverManager.setLoginTimeout(info.timeout);
    	    	conn = DriverManager.getConnection(url, username, password);
        	} catch (Exception e) {
        		e.printStackTrace();
        	}
        	if (conn != null) {
        		break;
        	}
        	try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
				LOG.error("sleep error!");
			}
    	}

    	return conn;
    }
    
    
    /**
     * @function 根据数据里连接名称获取一个连接
     * @param ds 数据库连接名
     * @return
     */
    public static Connection getConnection(String ds) {
    	if (CommonUtil.isNullOrEmpty(ds)) {
    		LOG.debug("ds name is null or empty!");
    		return null;
    	}
    	DSinfo info = dsmap.get(ds);
    	if (info == null) {
    		LOG.debug("ds info is null");
    		return null;
    	}
    	return getConnection(info);
    }

    /**
     * @function 初始化配置文件中配置的所有数据库信息
     */
    public static void initDS() {
    	dsmap = new HashMap<String, DSinfo>();
    	String prefix = Common.DS;
    	int num = ConfigFactory.getInt(prefix + Common.DSNUM, 0);
    	String baseStr = prefix + ".item(0)";
    	for (int i = 0; i < num; i++) {
    		String str = baseStr.replaceAll("0", String.valueOf(i));
    		DSinfo info = new DSinfo();
    		
    		info.name = ConfigFactory.getString(str + Common.NAME);
    		info.driver = ConfigFactory.getString(str + Common.DRIVERNAME);
    		info.url = ConfigFactory.getString(str + Common.URL);
    		info.username = ConfigFactory.getString(str + Common.USERNAME);
    		info.password = ConfigFactory.getString(str + Common.PASSWORD);
    		info.timeout = ConfigFactory.getInt(str + Common.TIMEOUT, 10);
    		dsmap.put(info.name, info);
    		try {
				Class.forName(info.driver);
			} catch (ClassNotFoundException e) {
				LOG.error("{}", e);
			} 
    	}
		LOG.info("init database info success!");
    }
    

	/**
	 * @function 关闭数据库Statement
	 * @param stmt 带关闭的Statement
	 */
	public static void close(Statement stmt) {
		try {
			if (stmt != null) {
				stmt.close();
				stmt = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @function 关闭数据库Statement
	 * @param pstmt 带关闭的Statement
	 */
	public static void close(PreparedStatement pstmt) {

		try {
			if (pstmt != null) {
				pstmt.close();
				pstmt = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @function 关闭数据库结果集
	 * @param rst 数据库结果集
	 */
	public static void close(ResultSet rst) {
		try {
			if (rst != null) {
				rst.close();
				rst = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @function 关闭数据库连接
	 * @param conn 数据库连接
	 */
	public static void close(Connection conn) {
		try {
			if (conn != null) {
				conn.close();
				conn = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param sqlList 要执行的SQL语句集合
	 * @param tableName	
	 * @param filename
	 * @return
	 */
	/*
	public boolean importData(List<String> sqlList, String tableName
			,String filename) {
		try {
			Connection conn = getConnection();
			if (conn == null) {
				LOG.error("Connection of DB is NULL");
				return false;
			}
			List<String> errSqlList = new ArrayList<String>();
			Statement stmt = null;
			try {
				conn.setAutoCommit(false);
				stmt = conn.createStatement();

				int limit = ConfigFactory.getInt("common.sql_batch", 10000);
				int start = 0;
				int end = start + limit;

				if (end > sqlList.size()) {
					end = sqlList.size();
				}
				List<String> newlist = sqlList.subList(start, end);

				while (newlist.size() > 0) {
					for (String s : newlist) {
						stmt.addBatch(s);
					}
					try {
						stmt.executeBatch();
						conn.commit();
					} catch (BatchUpdateException e) {
						LOG.error("{}", e);
						int[] retcodes = e.getUpdateCounts();
						for (int i = 0; i < retcodes.length; i++) {
							int retcode = retcodes[i];
							if (retcode == Statement.EXECUTE_FAILED) {
								errSqlList.add(newlist.get(i));
							}
						}
						try {
							conn.commit();
						} catch (Exception e2) {
							LOG.error("{}", e2);
						}
						
					} catch (Exception e) {
						LOG.error("{}", e);
					}

					start = end;
					end += limit;

					if (start >= sqlList.size()) {
						break;
					}
					if (end > sqlList.size()) {
						end = sqlList.size();
					}

					newlist = sqlList.subList(start, end);
				}
				
			} catch (Exception e) {
				LOG.error("{}", e);
				return false;
			} finally {
				try {
					close(stmt);
					close(conn);
				} catch (SQLException e2) {
					LOG.error("{}", e2);
				}
			}
			int errCount = errSqlList.size();
			LOG.info(filename + ":" + tableName + "错误数据:" + errCount);
			if (errCount > 0) {
				IOUtil.writeToCoreFile(errSqlList);
				
			}
		} catch (Exception e) {
			LOG.error("{}", e);
			return false;
		}
		return true;
	}
*/

	/**
	 * @param sqlList 要执行的SQL语句集合
	 * @param ds 数据库连接名称
	 * @return 执行结果
	 */
	public static boolean executeSql(List<String> sqlList, String ds) {
		if (CommonUtil.isNullOrEmpty(sqlList) || CommonUtil.isNullOrEmpty(ds)) {
			LOG.error("please check sqlList and ds");
			return false;
		}		
		
		try {
			Connection conn = getConnection(ds);
			if (conn == null) {
				LOG.error("Connection of DB is NULL");
				return false;
			}
			List<String> errSqlList = new ArrayList<String>();
			Statement stmt = null;
			try {
				conn.setAutoCommit(false);
				stmt = conn.createStatement();

				int limit = ConfigFactory.getInt("common.sql_batch", 10000);
				int start = 0;
				int end = start + limit;

				if (end > sqlList.size()) {
					end = sqlList.size();
				}
				List<String> newlist = sqlList.subList(start, end);

				while (newlist.size() > 0) {
					for (String s : newlist) {
						LOG.debug("sql: " + s);
						stmt.addBatch(s);
					}
					try {
						stmt.executeBatch();
						conn.commit();
					} catch (BatchUpdateException e) {
						LOG.error("{}", e.toString());
						int[] retcodes = e.getUpdateCounts();
						for (int i = 0; i < retcodes.length; i++) {
							int retcode = retcodes[i];
							if (retcode == Statement.EXECUTE_FAILED) {
								errSqlList.add(newlist.get(i));
							}
						}
						try {
							conn.commit();
						} catch (Exception e2) {
							LOG.error("{}", e2);
						}
					} catch (Exception e) {
						LOG.error("{}", e);
					}

					start = end;
					end += limit;

					if (start >= sqlList.size()) {
						break;
					}
					if (end > sqlList.size()) {
						end = sqlList.size();
					}

					newlist = sqlList.subList(start, end);
				}

			} catch (Exception e) {
				LOG.error("{}", e);
				return false;
			} finally {
				close(stmt);
				close(conn);
			}
			int errCount = errSqlList.size();
			if (errCount > 0) {
				LOG.warn("There are total {} sql statements execute error!", errSqlList.size());
				IOUtil.writeToCoreFile(errSqlList);
				
				if ((errSqlList.size() * 3) == sqlList.size()) {
					return false;
				}
			}
		} catch (Exception e) {
			LOG.error("{}", e);
			return false;
		}
		return true;
	}
	
	
	/**
	 * @function Clob转换成String 的方法
	 * @param clob Oracle数据库中的一种类型
	 * @return 转换后的String
	 */
	public static String clob2String(CLOB clob) {
		String content = null;
		StringBuffer stringBuf = new StringBuffer();
		Reader inStream = null;
		BufferedReader br = null;
		try {
			 inStream = clob.getCharacterStream(); // 取得大字侧段对象数据输出流
			 br = new BufferedReader(inStream);
			String s = br.readLine();
			
			while (s != null) {
				stringBuf.append(s);
				s = br.readLine();
			}
			content = stringBuf.toString();
		} catch (Exception ex) {
			LOG.error("{}", ex);
		} finally {
			try {
				inStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return content;
	}
	

	/**
	 * @param table  到地方
	 * @param fieldSet 的发生地
	 * @return
	 */
	public static String getInsertSql(String table, Set<String> fieldSet) {
		if (CommonUtil.isNullOrEmpty(table) || CommonUtil.isNullOrEmpty(fieldSet)) {
			LOG.error("get base SQL error, table name or field set is null or empty!");
			return null;
		}
		
		StringBuilder sb = new StringBuilder("insert into " + table + "(");
		for (String field : fieldSet) {
			sb.append(field).append(",");
		}
		sb.deleteCharAt(sb.length() - 1).append(") values(");
		
		for (int idx = 0; idx < fieldSet.size(); ++idx) {
			sb.append("?,");
		}
		sb.deleteCharAt(sb.length() - 1).append(")");
		
		return sb.toString();
	}
	
	
	
	/**
	 * @功能 ： 执行SQL语句获得的操作集合
	 * @param pstmt 要执行的SQL Statement
	 * @return ResultSet 执行结果
	 * @throws SQLException
	 */
	public static ResultSet getResultSet(PreparedStatement pstmt) {
		if (null == pstmt) {
			LOG.error("PreparedStatement of getResultSet in null");
			return null;
		}
		ResultSet rst = null;
		int tried = 0;	//已经尝试次数
		long startTime = System.currentTimeMillis();
		while (tried < MAXTRY) {
			try {
				 rst = pstmt.executeQuery();
				 break;
			} catch (Exception e) {
				LOG.error(tried + "th exec getResultSet failed!");
				++tried;
				close(rst);
			}
		}
		if (tried == MAXTRY) {
			LOG.error(tried + "th exec getResultSet failed! please check SQL connection!");
			return null;
		}

		long endTime = System.currentTimeMillis();
		LOG.info("exec getResultSet success! cost " + DateUtil.getReadableTime(endTime - startTime));
		return rst;
	}
	
//	private boolean setPreparedStatement(PreparedStatement pstmt, FieldConf fieldConf, 
//			Set<String> fieldSet, Map<String, String> map) {
//		if (null == pstmt) {
//			LOG.error("PreparedStatement parameter is null!");
//			return false;
//		}
//		if (null == fieldConf) {
//			LOG.error("FieldConf parameter is null!");
//			return false;
//		}
//		if (CommonUtil.isNullOrEmpty(fieldSet)) {
//			LOG.error("field set parameter is null or empty!");
//			return false;
//		}
//		if (CommonUtil.isNullOrEmpty(map)) {
//			LOG.error("field set parameter is null or empty!");
//			return false;
//		}
//		
//		try {
//			int index = 1;
//			for (String field : fieldSet) {				
//				String valueStr = map.get(field);
//				if (fieldConf.getIntset().contains(field)) {
//					int value = 0;
//					if (valueStr != null) {
//						value = Integer.parseInt(valueStr);
//					}
//					pstmt.setInt(index, value);
//				} else if (fieldConf.getLongset().contains(field)) {
//					long value = 0L;
//					if (valueStr != null) {
//						value = Long.parseLong(valueStr);
//					}
//					pstmt.setLong(index, value);
//				} else if (fieldConf.getFloatset().contains(field)) {
//					float value = 0f;
//					if (valueStr != null) {
//						value = Float.parseFloat(valueStr);
//					}
//					pstmt.setFloat(index, value);
//				} else if (fieldConf.getDatetimeset().contains(field)) {
//					Timestamp timeStamp = new Timestamp(0L);
//					if (valueStr != null) {
//						timeStamp = new Timestamp(Long.parseLong(valueStr));
//					}
//					pstmt.setTimestamp(index, timeStamp);
//				} else if (fieldConf.getTimeset().contains(field)) {
//					Time time = null;
//					if (valueStr != null) {
//						time = new Time(Long.parseLong(valueStr));
//					}
//					pstmt.setTime(index, time);
//				} else if (fieldConf.getTimestampset().contains(field)) {
//					Timestamp timeStamp = null;
//					if (valueStr != null) {
//						timeStamp = new Timestamp(Long.parseLong(valueStr));
//					}
//					pstmt.setTimestamp(index, timeStamp);
//				} else {
//					pstmt.setString(index, valueStr);
//				}
//				++index;
//			}
//		} catch (SQLException e) {
//			LOG.error("setPreparedStatement error!");
//			e.printStackTrace();
//			return false;
//		}
//		return true;
//	}
	
	/*
	public static void main(String[] argv) {
		String confpath = System.getProperty("datatransfer");
		if (confpath == null) {
			System.setProperty("datatransfer", Common.DEFAULTCONFIGPATH);
		}
		LogFactory.config("./conf/logback.xml");
		ConfigFactory.init("./conf/config.xml");
		
		//初始化数据库连接信息
		SQLUtil.initDS();
		
		//初始化全部配置变量
		int ret = GlobalVariable.init();

		String table = "dyjiaoliu";
		String sql = "insert into dyjiaoliu(uniqid) values('id_replace')";
		
		String[] ids = new String[]{"u001", "u002", "u003", "u001", "u004", "u002", "u005", "u006"};
		
		List<String> list = new ArrayList<String>();
		for (String id : ids) {
			System.out.println(id);
			list.add(sql.replace("id_replace", id));
		}

		SQLUtil.executeSql(list, GlobalVariable.tableConfMap.get(table).ds);
		
		
		System.out.println("success!");
		
	}
	*/
	
}
