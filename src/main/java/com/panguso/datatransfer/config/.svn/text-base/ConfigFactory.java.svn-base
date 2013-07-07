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
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;

/**
 * @author liubing
 *
 */
public final class ConfigFactory {
	private static final String CONFIG_FILE_DEFAULT_PATH = "./conf/config.xml";
	private static XMLConfiguration config = null;

	/**
	 * 构造函数
	 */
	private ConfigFactory() {
		
	}
	
	/**
	 * @param configFilePath config文件的相对路径
	 */
	public static void init(String configFilePath) {
		if (configFilePath == null) {
			configFilePath = CONFIG_FILE_DEFAULT_PATH;
		}
		try {
			//获取工程的本目录路径
			String path = System.getProperty("datatransfer");
			if (path == null) {
				path = "";
			}
			config = new XMLConfiguration(path + File.separator
					+ configFilePath);
			config.setReloadingStrategy(new FileChangedReloadingStrategy());
		} catch (ConfigurationException e) {
			System.out.println("Fatal:Create Config Object Error!!!");
			System.exit(1);
		}
	}

	
	/**
	 * @function 根据congig标签从config文件中获取对应标签的字符串值
	 * @param configXPath 待读取的标签路径
	 * @return 标签对应的值
	 */
	public static String getString(String configXPath) {
		return config.getString(configXPath, null);
	}

	/**
	 * @param configXPath 待读取的标签路径
	 * @param defaultValue 默认值
	 * @return 标签对应的值
	 */
	public static String getString(String configXPath, String defaultValue) {
		return config.getString(configXPath, defaultValue);
	}

	/**
	 * @param configXPath 待读取的标签路径
	 * @return 标签对应的值
	 */
	public static int getInt(String configXPath) {
		return config.getInt(configXPath);
	}

	/**
	 * @param configXPath 待读取标签的路径
	 * @param defaultValue 默认值
	 * @return 标签对应的整数值
	 */
	public static int getInt(String configXPath, int defaultValue) {
		return config.getInt(configXPath, defaultValue);
	}

	/**
	 * @param configXPath 待读取标签的路径
	 * @return 标签对应的浮点值
	 */
	public static float getFloat(String configXPath) {
		return config.getFloat(configXPath, 1.0F);
	}

	/**
	 * @param configXPath 待读取的标签的路径
	 * @return 标签对应的值
	 */
	public static boolean getBoolean(String configXPath) {
		return config.getBoolean(configXPath);
	}

	/**
	 * @param configXPath 待读取标签的路径
	 * @param defaultValue 默认值
	 * @return 标签对应的布尔值
	 */
	public static boolean getBoolean(String configXPath, boolean defaultValue) {
		return config.getBoolean(configXPath, defaultValue);
	}

	/**
	 * @param configXPath 待读取的标签路径
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static List<String> getList(String configXPath) {
		return config.getList(configXPath);
	}
}