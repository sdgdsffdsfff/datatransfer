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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

/**
 * @author liubing
 *
 */
public final class LogFactory {
	
	private LogFactory() {
		
	}
	
	/**
	 * 初始化
	 * @param configFilePath 配置文件路径
	 */
	public static void config(String configFilePath) {
		String path = System.getProperty("datatransfer");
		if (path == null) {
			path = "";
		}		
		LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
		JoranConfigurator configurator = new JoranConfigurator();
		configurator.setContext(lc);
		lc.reset();
		try {
			configurator.doConfigure(path + File.separator + configFilePath);
		} catch (JoranException e) {
			System.out.println("Fatal: Init LogConfig Error.");
			System.exit(1);
		}
	}

	/**
	 * 打印DEBUG信息
	 * @param loggerName 
	 * @param msg 
	 */
	public static void debug(String loggerName, String msg) {
		Logger log = LoggerFactory.getLogger(loggerName);
		log.debug(msg);
	}

	/**
	 * 打印通知信息
	 * @param loggerName 
	 * @param msg 
	 */
	public static void info(String loggerName, String msg) {
		Logger log = LoggerFactory.getLogger(loggerName);
		log.info(msg);
	}

	/**
	 * 打印警告信息
	 * @param loggerName 
	 * @param msg 
	 */
	public static void warn(String loggerName, String msg) {
		Logger log = LoggerFactory.getLogger(loggerName);
		log.warn(msg);
	}

	/**
	 * 打印错误信息
	 * @param loggerName 
	 * @param msg 
	 * @param e 
	 */
	public static void error(String loggerName, String msg, Throwable e) {
		Logger log = LoggerFactory.getLogger(loggerName);
		log.error(msg, e);
	}
}