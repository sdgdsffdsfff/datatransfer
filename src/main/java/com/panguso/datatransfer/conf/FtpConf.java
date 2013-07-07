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

import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;
import com.panguso.datatransfer.util.CommonUtil;

/**
 * @author liubing
 *
 */
public class FtpConf {
	private static final Logger LOG = LoggerFactory.getLogger(FtpConf.class); 
	private String type = "ftp";		//下载类型
	private String host = null;			//主机
	private int port = 21;				//端口号
	private String userName = null;		//用户名
	private String passWord = null;		//密码
	private String encode = "utf-8";	//编码
	private boolean issupportbroken = false;	//是否支持断点续传
	private FtpDownloadConf ftpDownloadConf = null;	//下载相关的其他配置信息
	private String prefix = null;
	/**
	 * 构造函数
	 * @param prefix 前缀
	 */
	public FtpConf(String prefix) {
		this.prefix = prefix + Common.FTPPREFIX;
	}

	/**
	 * 初始化方法
	 */
	public void init() {
		//初始化FTP参数 下载类型(ftp 或  sftp)
		this.type = ConfigFactory.getString(prefix + Common.TYPE, "ftp");
		
		//初始化FTP参数 主机号
		String hostStr = ConfigFactory.getString(prefix + Common.HOST);
		if (CommonUtil.isNullOrEmpty(hostStr)) {
			LOG.error("ERROR:init FtpConf failed, config value for host tag is null or empty!");
			System.exit(-1);
		}
		this.host = hostStr;

		//初始化FTP参数 端口号
		int iPort = ConfigFactory.getInt(prefix + Common.PORT, 21);
		if (iPort == Common.FTPPORT || iPort == Common.SFTPPORT) {
			this.port = iPort;
		} else {
			LOG.error("ERROR:init FtpConf failed, config value for port tag is empty or wrong!");
			System.exit(-1);
		}
		
		//初始化FTP参数 用户名
		String username = ConfigFactory.getString(prefix + Common.USERNAME, "");
		if (username.isEmpty()) {
			LOG.warn("WARN:init FtpConf warning: config value for username tag is empty!");
		} 
		this.userName = username;

		
		//初始化FTP参数 密码
		String password = ConfigFactory.getString(prefix + Common.PASSWORD, "");
		if (password.isEmpty()) {
			LOG.warn("WARN:init FtpConf warning: config value for password tag is empty!");
		}
		this.passWord = password;
		
		//配置FTP参数 编码格式
		String encodeStr = ConfigFactory.getString(prefix + Common.ENCODE, "utf-8");
		this.encode = encodeStr;
		
		//配置FTP参数 断点续传
		Boolean support = ConfigFactory.getBoolean(prefix + Common.ISSUPPORTBROKEN, false);
		this.issupportbroken = support;
		
		//初始化FTP下载路径相关参数
		ftpDownloadConf = new FtpDownloadConf(prefix);
		ftpDownloadConf.init();
		LOG.warn("init FtpDownloadConf for " + prefix + " success!");
	}
	
	
	
	public String getType() {
		return type;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getUsername() {
		return userName;
	}

	public String getPassword() {
		return passWord;
	}

	public String getEncode() {
		return encode;
	}

	public boolean isIssupportbroken() {
		return issupportbroken;
	}

	public FtpDownloadConf getFtpDownloadConf() {
		return ftpDownloadConf;
	}
	
}
