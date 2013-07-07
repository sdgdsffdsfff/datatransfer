/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.util.CommonUtil;
import com.panguso.datatransfer.util.DateUtil;

/**
 * @author liubing
 *
 */
public final class Common {
	private static final Logger LOG = LoggerFactory.getLogger(Common.class);	
	private static Properties prop = new Properties();
	private static final String prop_file = "conf/laststamp.properties";
	private static final int ERROR = -1;
	private static final int SUCC = 1;
	
	/**
	 * 构造函数
	 */
	private Common() {
		
	}
	
	
	//增量数据相关参数
	/**
	 * 增量数据里的操作字段之一 添加
	 */
	public static final int ADD = 1;
	/**
	 * 增量数据里的操作字段之一 修改
	 */
	public static final int UPDATE = 2;
	/**
	 * 增量数据里的操作字段之一 删除
	 */
	public static final int DELETE = 3;
	
	//关于属性文件存储相关TAG
	/**
	 * 对于增量文件要存储的序号标签
	 */
	public static final String INDEX = "index";
	/**
	 * 对于增量文件要存储的时间标签
	 */
	public static final String STAMP = "stamp";
	
	/**
	 * 数据库操作每次取数据条数的默认值
	 */
	public static final int DEFAULTLIMIT = 10000;
	
	/**
	 *	 建立索引时等待其他job的最多次数
	 */
	public static final int INDEX_MAX_SLEEP_CNT = 2 * 60 * 60;
	/**
	 * 建立索引时等待其他job的每次等待时间
	 */
	public static final int INDEX_PRE_SLEEP_TIME = 10 * 1000;
	
	/**
	 * job的默认工作类（主要工作是传前缀参数）
	 */
	public static final String DEFAULTJOBCLASS = "com.panguso.datatransfer.job.CommonDataTransferJob";
	
	//分隔符
	/**
	 * 序列分隔符
	 */
	public static final String SEPARATOR = ";";
	/**
	 * MAP对应的分隔符
	 */
	public static final String MAPSEPARATOR = ":";
	/**
	 * 文件名中与后缀相关的分隔符
	 */
	public static final String DOT = ".";
	/**
	 * 二级序列的分隔符
	 */
	public static final String COMMA = ",";
	
	/**
	 * LOCK文件的后缀名
	 */
	public static final String LOCK = ".lock"; 
	
	/**
	 * DONE文件的后缀名
	 */
	public static final String DONE = ".done";
	
	/**
	 * doing文件的后缀名
	 */
	public static final String DOING = ".doing";
	
	
	//公共部分
	/**
	 * 一个处理过程失败后的休眠时间标签
	 */
	public static final String SLEEP = "common.sleep";
	/**
	 * 一个过程失败后继续尝试的最大尝试次数标签
	 */
	public static final String MAXTRY = "common.max_try";
	/**
	 * 	数据库操作每次操作行数数量限制数目
	 */
	public static final String SQLLIMIT = "common.sql_limit";
	
	/**
	 *	该工程的根目录的绝对路径 
	 */
	public static final String CONFIGPATH = "common.config_path";
	
	/**
	 * 默认的最大尝试次数
	 */
	public static final int DEFAULTMAXTRY = 3;
	/**
	 * 默认休眠的时间长度 
	 */
	public static final long DEFAULTSLEEPTIME = 10 * 1000;
	
	//bigjob相关TAG	
	/**
	 * 全量接入的job总标签
	 */
	public static final String BIGJOBS = "bigjobs";
	/**
	 * 全量接入的job标签
	 */
	public static final String BIGJOB = ".bigjob";
	/**
	 * 增量接入的job总标签
	 */
	public static final String DYJOBS = "dyjobs";
	/**
	 * 增量接入的JOB标签
	 */
	public static final String DYJOB = ".dyjob";
	
	/**
	 * 索引job的总标签
	 */
	public static final String INDEXJOBS = "indexjobs";
	/**
	 * 索引job的标签
	 */
	public static final String INDEXJOB = ".indexjob";
	
	/**
	 * 全量索引的job名称
	 */
	public static final String BIGINDEX = "bigindex";
	/**
	 * 增量索引的job名称
	 */
	public static final String DYINDEX = "dyindex";
	
	
	/**
	 * job的ID属性
	 */
	public static final String ATTRIBUTE = "[@id]";
	/**
	 * job组的job个数
	 */
	public static final String JOBNUMBER = ".job_num";
	
	/**
	 * job的索引执行shell脚本
	 */
	public static final String INDEXCMD = ".indexcmd";
	
	
	/**
	 * 各种名称Tag
	 */
	public static final String NAME = ".name";
	/**
	 * job类型（枚举，ftp、db、index）
	 */
	public static final String TYPE = ".type";
	/**
	 * job的处理类
	 */
	public static final String JOBCLASS = ".job_class";
	/**
	 * job的下载类
	 */
	public static final String DOWNLOADCLASS = ".download_class";
	/**
	 * job的数据导入类
	 */
	public static final String IMPORTCLASS = ".import_class";
	/**
	 * job的索引类
	 */
	public static final String INDEXCLASS = ".index_class";
	
	/**
	 * 建立索引需要等待的job名称
	 */
	public static final String WAITFORJOBS = ".wait_for_jobs";
	/**
	 * 建立索引是否需要等待job
	 */
	public static final String WAITFORJOBENABLE = ".wait_for_jobs_enable";
	/**
	 * 建立全量索引时需要处理的数据表
	 */
	public static final String INVOLVETABLES = ".involve_tables";
	
	/**
	 * 标识数据接入过程多个步骤的开关命令
	 */
	public static final String CMD = ".cmd";
	/**
	 * 默认数据接入多步骤的开关命令值
	 */
	public static final String DEFAULTCMD = "000";
	/**
	 * 命令长度
	 */
	public static final int CMDLEN = 3;
	/**
	 * 调度时间
	 */
	public static final String CRONTAB = ".crontab";
	/**
	 * 数据接入文件编码类型
	 */
	public static final String FILEENCODE = ".encode";
	/**
	 * 数据文件中每行的字段分隔符
	 */
	public static final String FIELDSEPARATOR = ".separator";	
	
	//table相关TAG
	/**
	 * 配置文件中数据表的总标签
	 */
	public static final String TABLES = "tables";
	/**
	 * 总共的数据表个数
	 */
	public static final String TABLENUM = ".table_num";
	/**
	 * 每个表的标签
	 */
	public static final String TABLE = ".table";
	/**
	 * 操作表前缀
	 */
	public static final String OPERATIONPREFIX = "operate_log_";
	/**
	 * 全量表前缀
	 */
	public static final String FULLTABLESUFFIX = "";
	/**
	 * 增量表前缀
	 */
	public static final String DYTABLESUFFIX = "dy";
	/**
	 * 对操作表最后操作的ID的后缀
	 */
	public static final String LASTOPIDSUFFIX = "_lastoperateid";
	
	
	//ds相关TAG
	/**
	 * 数据库连接信息的总标签
	 */
	public static final String DS = "ds";
	/**
	 * 数据库连接信息的个数
	 */
	public static final String DSNUM = ".ds_num";
	/**
	 * 数据库连接url
	 */
	public static final String URL = ".url";
	/**
	 * 数据库连接超时时间
	 */
	public static final String TIMEOUT = ".timeout";
	/**
	 * 数据库连接驱动类
	 */
	public static final String DRIVERNAME = ".driverClassName";
	
	//下载类型
	/**
	 * @author liubing
	 * job类型，包括ftp/db接入、index三种类型
	 */
	public final class JobType {
		/**
		 * FTP下载类型
		 */
		public static final String FTP = "ftp";
		/**
		 * 数据库下载类型
		 */
		public static final String DB = "db";
		
		/**
		 * 索引类型
		 */
		public static final String INDEX = "index";
		private JobType() {
		}
	}
	
	
	/**
	 * 全量方式接入的job名称前缀
	 */
	public static final String BIG = "big";
	/**
	 * 增量接入方式的job名称前缀
	 */
	public static final String DY = "dy";
	
	//配置文件数据表相关TAG
	/**
	 * 数据表的主键名称
	 */
	public static final String PRIMARYKEY = ".primay_key";
	/**
	 * 该数据表所属的数据库连接名称
	 */
	public static final String DB = ".ds";
	/**
	 * 该表每个字段的长度
	 */
	public static final String FIELDLEN = ".field_length";
	/**
	 * 该表在接入时要跳过的忽略字段
	 */
	public static final String FIELDSKIP = ".field_skip";
	/**
	 * 该表在实际接入时要映射的字段
	 */
	public static final String FIELDMAP = ".field_map";
	/**
	 * 索引类型映射tag
	 */
	public static final String TABLEINDEXTYPEMAP = ".index_type_map";
	/**
	 * 该表的字段列表
	 */
	public static final String FIELD = ".fields";

	//FTP参数相关
	/**
	 * FTP总标签
	 */
	public static final String FTPPREFIX = ".ftp"; 
	/**
	 * FTP服务器主机名称
	 */
	public static final String HOST = ".host"; 
	/**
	 * FTP服务器端口号
	 */
	public static final String PORT = ".port";
	/**
	 * FTP登陆用户名
	 */
	public static final String USERNAME = ".username";
	/**
	 * FTP登陆密码
	 */
	public static final String PASSWORD = ".password";
	/**
	 * FTP的编码
	 */
	public static final String ENCODE = ".encode";
	/**
	 * 是否支持断点续传
	 */
	public static final String ISSUPPORTBROKEN = ".issupportbroken";
	/**
	 * FTP端口号
	 */
	public static final int FTPPORT = 21;
	/**
	 * SFTP端口号
	 */
	public static final int SFTPPORT = 22;	
	
	//FTP接入方式相关标签
	/**
	 * FTP服务器待下载的根路径
	 */
	public static final String REMOTEPATH = ".remotepath";
	/**
	 * 该接入方式要下载到的本地路径
	 */
	public static final String LOCALPATH = ".localpath";
	/**
	 * 是否配置下载目录开关
	 */
	public static final String DOWNDIRSWITCH = ".downloadDicEnable";
	/**
	 * 要下载的所有目录
	 */
	public static final String DIR = ".dics";
	/**
	 * 待下载文件标签后缀
	 */
	public static final String DOWNLOADFILE = "_downloadfile";
	/**
	 * 待下载的数据文件后缀
	 */
	public static final String DATAFILESUFFIX = ".data_suffix";
	/**
	 * MD5文件的后缀
	 */
	public static final String MD5SUFFIX = "md5";
	/**
	 * 下载完成文件的后缀
	 */
	public static final String DONEFILESUFFIX = "done";
	/**
	 * 文件名与待入库的数据表的映射
	 */
	public static final String FILETABLEMAP = ".file_table_map";
	/**
	 * 文件入库时要忽略的字段
	 */
	public static final String FILESKIP = ".file_skip";	
	/**
	 * 文件要入库到表时对应的主键字段
	 */
	public static final String FILEPRIMARYKEY = ".file_primary_field";	
	
	//DB下载参数 
	/**
	 * 标识DB接入方式的标签
	 */
	public static final String DBPREFIX = ".db";
	/**
	 * 数据来源表（对方）
	 */
	public static final String FROMTABLE = ".from_table";
	/**
	 * 数据接入表（本地）
	 */
	public static final String TOTABLE = ".to_table";
	/**
	 * 接入时要忽略掉的字段列（对方）
	 */
	public static final String SKIP = "_skip";
	/**
	 * 接入时要映射的列
	 */
	public static final String MAP = "_map";
		
	//正则表达式
	/**
	 * 数字串
	 */
	public static final String REGEXNUMBER = "\\d+";  //数字串
	
	/**
	 * yyyyMMdd格式
	 */
	public static final String REGEXDAY = "[0-9]{8}+";  //yyyyMMdd格式
	
	/**
	 * yyyyMMddHHmmss格式
	 */
	public static final String REGEXTIME = "[0-9]{14}+";  //yyyyMMddHHmmss格式
	
	
	
	
	/**
	 * 正整数
	 */
	public static final String REGEXINTEGER = "[1-9]\\d*";  //正整数
	/**
	 * 01数字串
	 */
	public static final String REGEXCMD = "[01]+";   //命令
	
	
    /**
     * @param key 要保存的属性键
     * @param value 要保存的属性值
     */
    public static void saveLastStamp(String key, String value) { 
      //  Properties prop = new Properties(); 
		String filePath = System.getProperty("datatransfer");
		filePath = filePath + File.separator + prop_file;
      
        try { 
        	   InputStream fis = new FileInputStream(filePath); 
               //从输入流中读取属性列表（键和元素对） 
               prop.load(fis); 
               //强制要求为属性的键和值使用字符串。返回值是 Hashtable 调用 put 的结果。 
               OutputStream fos = new FileOutputStream(filePath); 
               prop.setProperty(key, value); 
               //以适合使用 load 方法加载到 Properties 表中的格式， 
               //将此 Properties 表中的属性列表（键和元素对）写入输出流 
               prop.store(fos, "Update '" + key + "' value"); 
               LOG.info("have save properties, key: " + key + "  value: " + value);
           } catch (IOException e) { 
        	   LOG.error("Visit " + filePath + " for updating " + key + " value error"); 
           } 
       } 
	
	
	/**
	 * @return 初始化成功与否
	 */
	public static int initLastStamp() {
		String path = System.getProperty("datatransfer");
		String file = path + File.separator + prop_file;
		prop = new Properties();
		File f = new File(file);
		FileReader fr = null;
		try {
			if (!f.exists()) {
				f.createNewFile();
			}

			fr = new FileReader(f);
			prop.load(fr);
		} catch (Exception e) {
			LOG.error("{}", e);
			return ERROR;
		} finally {
			try {
				if (fr != null) {
					fr.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return SUCC;
	}	
	
	/**
	 * @function 针对增量FTP方式，记录上次处理的最后的时间值
	 * @param key 要查找的属性的键
	 * @return 对应的属性的值
	 */
	public static String getLaststamp(String key) {
		if (null == prop) {
			initLastStamp();
		}
		
		String strstamp = prop.getProperty(key);
		if (!CommonUtil.isNullOrEmpty(strstamp)) {
			return strstamp;
		}
		return DateUtil.getToday();
	}
	
	
	/**
	 * @function 针对增量DB方式，记录上次处理的最后的operateID
	 * @param key 要查找的属性的键
	 * @return 对应的属性的值
	 */
	public static String getLastOperateID(String key) {
		if (null == prop) {
			initLastStamp();
		}
		
		String strstamp = prop.getProperty(key);
		if (!CommonUtil.isNullOrEmpty(strstamp)) {
			return strstamp;
		}
		return "0";
	}
	
//	public static void main(String[] argv) {
//		String str1="20130504";
//		String str2="2013050400";
//		String str3="20130504000000";
//		
//		System.out.println(str3.matches(Common.REGEXTIME));
//		
//	}
	
}
