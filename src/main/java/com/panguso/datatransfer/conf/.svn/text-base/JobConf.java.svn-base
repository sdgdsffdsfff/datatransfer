/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;
import com.panguso.datatransfer.util.CommonUtil;

/**
 * @author liubing
 * 
 */
public class JobConf {
	private static final Logger LOG = LoggerFactory.getLogger(JobConf.class);
	private String jobName = null;			//job名称
	private String type = null;				//job类型（ftp、db,index）
	private String jobClass = null;			//job处理类
	private String downloadClass = null;	//job下载类
	private String importClass = null;		//job入库类
	private String indexClass = null;		//job创建索引类
	private String cmd = null;				//job各流程执行命令
	private String crontab = null;			//job处理周期
	private String encode = "utf-8"; 		//文件的编码格式
	private String fieldSeparator = "0x01";	//文件的字段分隔符号
	private FtpConf ftpConf = null;			//FTP接入方式对应的FTP相关信息
	private DBDownloadConf dbConf = null;	//DB接入方式，DB相关信息
	private IndexConf indexConf = null; 	//Index相关信息
	private Map<String, String> tableIndextypeMap = null;  //数据接入表与其在index_struct表中indextype的映射
	private String prefix = null;	//XPath前缀
	
	public String getJobName() {
		return jobName;
	}


	public String getType() {
		return type;
	}


	public String getJobClass() {
		return jobClass;
	}


	public String getDownloadClass() {
		return downloadClass;
	}


	public String getImportClass() {
		return importClass;
	}

	public String getIndexClass() {
		return indexClass;
	}
	
	public String getCmd() {
		return cmd;
	}


	public String getCrontab() {
		return crontab;
	}


	public String getEncode() {
		return encode;
	}


	public String getFieldSeparator() {
		return fieldSeparator;
	}


	public FtpConf getFtpConf() {
		return ftpConf;
	}


	public DBDownloadConf getDbConf() {
		return dbConf;
	}

	public IndexConf getIndexConf() {
		return indexConf;
	}
	
	public Map<String, String> getTableIndextypeMap() {
		return tableIndextypeMap;
	}
	
	/**
	 * 构造函数
	 * @param prefix JOB前缀
	 */
	public JobConf(String prefix) {
		this.prefix = prefix;
	}

	/**
	 * Job初始化方法
	 */
	public void init() {		
		//初始化job名称,必须是以big 或  dy开始
		this.jobName = ConfigFactory.getString(prefix + Common.NAME, "");
		if (jobName.isEmpty() || !(jobName.startsWith(Common.BIG) || jobName.startsWith(Common.DY))) {
			LOG.error("config value for name tag of {} is empty or error!", jobName);
			System.exit(-1);
		}
		
		LOG.info("Begin to init JobConf of {}...", jobName);
		//初始化job处理类
		String jobclass = ConfigFactory.getString(prefix + Common.JOBCLASS);
		if (!CommonUtil.isNullOrEmpty(jobclass)) {
			this.jobClass = jobclass.trim();
		} else {
			this.jobClass = Common.DEFAULTJOBCLASS;
			LOG.warn("init {} job warn：no config for job_class tag!", jobName);
		}
		
		//初始化命令cmd
		String cmdStr = ConfigFactory.getString(prefix + Common.CMD);
		if (!CommonUtil.isNullOrEmpty(cmdStr) && cmdStr.matches(Common.REGEXCMD)
				&& cmdStr.length() == Common.CMDLEN) {
			this.cmd = cmdStr.trim();
		} else {
			this.cmd = Common.DEFAULTCMD;
			LOG.warn("init {} warn: no config value for cmd tag, using default cmd value: {}", jobName, cmd);
		}
		
		//初始化下载类
		if (isDownloadConfiged()) {
			String downloadclass = ConfigFactory.getString(prefix + Common.DOWNLOADCLASS);
			if (!CommonUtil.isNullOrEmpty(downloadclass)) {
				this.downloadClass = downloadclass.trim();
			} else {
				LOG.error("init {} error：no config value for download_class tag!", jobName);
				System.exit(-1);
			}
		}
		
		//初始化导入类
		if (isImportConfiged()) {
			String importclass = ConfigFactory.getString(prefix + Common.IMPORTCLASS);
			if (!CommonUtil.isNullOrEmpty(importclass)) {
				this.importClass = importclass.trim();
			} else {
				LOG.error("init {} error：no config value for import_class tag!", jobName);
				System.exit(-1);
			}
		}

		//初始化索引类
		if (isIndexConfiged()) {
			//索引命令被置位时要确保本job是索引job
			if (!CommonUtil.isIndexJob(jobName)) {
				LOG.error("{} is not an index job, should not config index class!");
				System.exit(-1);
			}
			//获取索引类
			String indexclass = ConfigFactory.getString(prefix + Common.INDEXCLASS);
			if (!CommonUtil.isNullOrEmpty(indexclass)) {
				this.indexClass = indexclass.trim();
			} else {
				LOG.error("init {} error：no config value for index_class tag!", jobName);
				System.exit(-1);
			}
		}
		
		//初始化Crontab
		String crontabStr = ConfigFactory.getString(prefix + Common.CRONTAB);
		if (CommonUtil.isNullOrEmpty(crontabStr)) {
			LOG.error("init {} error: no config value for crontab tag!", jobName);
			System.exit(-1);
		}
		this.crontab = crontabStr;	
		
		//初始化job下载类型，索引job的类型默认为index
		String typeStr = "";
		if (CommonUtil.isIndexJob(jobName)) {
			typeStr = Common.INDEX;
		} else {
			typeStr = ConfigFactory.getString(prefix + Common.TYPE, "");
		}
		
		//根据job类型初始化jobConf中的FTPConf DBConf IndexConf
		if (!typeStr.isEmpty()) {
			this.type = typeStr.trim().toLowerCase();
			if (type.equalsIgnoreCase(Common.JobType.FTP)) {
				this.initFTPConf();
			} else if (type.equalsIgnoreCase(Common.JobType.DB)) {
				this.initDBConf();
			} else if (type.equalsIgnoreCase(Common.JobType.INDEX)) {
				this.initIndexConf();
			}
		} else {
			LOG.error("init {} error: no config value for type tag!", jobName);
			System.exit(-1);
		}

		LOG.info("init JobConf of {} success!", jobName);
	}
	
	
	/**
	 * @function  初始化jobConf中的DBConf
	 * @return 无
	 */
	private void initDBConf() {
		dbConf = new DBDownloadConf(prefix);
		dbConf.init();
		
		//如果是增量DB类型job,从db的配置项中获取tableIndextypeMap
		if (CommonUtil.isDyJob(jobName)) {
			String tableIndextypeMapTag = prefix + Common.DBPREFIX + Common.TABLEINDEXTYPEMAP;
			tableIndextypeMap = CommonUtil.getMapFromTag(tableIndextypeMapTag);
		}
	}
	
	/**
	 * @function 初始化jobConf中的FTPConf
	 * @return 无
	 */
	private void initFTPConf() {
		//初始化只有FTP方式job才有的文件编码方式
		String encodeStr = ConfigFactory.getString(prefix + Common.FILEENCODE);
		if (CommonUtil.isNullOrEmpty(encodeStr)) {
			LOG.error("init {} warn: no config value for encode tag, using default encode value！", jobName);
			System.exit(-1);
		} else {
			this.encode = encodeStr.trim();
		}
		
		//初始化只有FTP方式job字段分隔符
		String separator = ConfigFactory.getString(prefix + Common.FIELDSEPARATOR);
		if (CommonUtil.isNullOrEmpty(separator)) {
			LOG.error("init {} error: no config value for separator tag! using default separator: 0x01", jobName);
			System.exit(-1);
		} else {
			if (separator.startsWith("0x")) {
				if (Integer.decode(separator) < 128) {
					this.fieldSeparator = new String(new byte[] {Byte.decode(separator)});
				} else {
					LOG.info("separator config for {} error, range of separator is [0x0 - 0x7F]");
					System.exit(-1);
				}
			} else {
				this.fieldSeparator = separator;
			}
		}
		
		ftpConf = new FtpConf(prefix);
		ftpConf.init();
		
		//如果是增量FTP类型job,从ftp的配置项中获取tableIndextypeMap
		if (CommonUtil.isDyJob(jobName)) {
			String tableIndextypeMapTag = prefix + Common.FTPPREFIX + Common.TABLEINDEXTYPEMAP;
			tableIndextypeMap = CommonUtil.getMapFromTag(tableIndextypeMapTag);
		}
	}
	
	/**
	 * 初始化jobConf中的indexConf
	 */
	private void initIndexConf() {
		indexConf = new IndexConf(prefix, jobName);
		indexConf.init();
	}
		
	/**
	 * 判断本job是否配置了导入类
	 * @return
	 */
	private boolean isDownloadConfiged() {
		if (CommonUtil.isNullOrEmpty(cmd)) {
			return false;
		}
		return (cmd.charAt(0) != '0');
	}	
	
	
	/**
	 * 判断本job是否配置了导入类
	 * @return
	 */
	private boolean isImportConfiged() {
		if (CommonUtil.isNullOrEmpty(cmd)) {
			return false;
		}
		return (cmd.charAt(1) != '0');
	}
	
	/**
	 * 判断本job是否配置了导入类
	 * @return
	 */
	private boolean isIndexConfiged() {
		if (CommonUtil.isNullOrEmpty(cmd)) {
			return false;
		}
		return (cmd.charAt(2) != '0');
	}
}
