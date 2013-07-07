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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author liubing
 *
 */
public final class PropertyFactory {
	
	private static Properties properties = new Properties();
	private static final String propertyFileName = "config.properties";
	private static final String FILE_DATE_PSIS_FILE = "filedate.dat";
	private static final String REDO_JOBS_PSIS_FILE = "redojobs.dat";
	private static HashMap<String, Date> fileDate = null;
	private static HashMap<String, Boolean> redoJobs = null;
	private static ExecutorService pool = null;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	
	private PropertyFactory() {
		
	}
	
	/**
	 * 初始化
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	@SuppressWarnings("unchecked")
	public static void init() throws IOException, ClassNotFoundException {
		String path = System.getProperty("datatransfer");
		if (path == null) {
			path = "";
		}
		// 加载属�?配置文件
		InputStream in = new BufferedInputStream(new FileInputStream(path
				+ File.separator + propertyFileName));
		properties.load(in);

		// 加载日期配置文件
		File filedatefile = new File(path + File.separator
				+ FILE_DATE_PSIS_FILE);
		if (null != filedatefile && filedatefile.exists()) {
			ObjectInputStream datain = new ObjectInputStream(
					new FileInputStream(filedatefile));
			Object myobj = datain.readObject();
			fileDate = (HashMap<String, Date>) myobj;
			in.close();
			datain.close();
		} else {
			fileDate = new HashMap<String, Date>();
		}

		// 读取redo标识文件
		File redojobsfile = new File(path + File.separator
				+ REDO_JOBS_PSIS_FILE);
		if (null != redojobsfile && redojobsfile.exists()) {
			ObjectInputStream datain = new ObjectInputStream(
					new FileInputStream(redojobsfile));
			Object redoobj = datain.readObject();
			redoJobs = (HashMap<String, Boolean>) redoobj;
			in.close();
			datain.close();
		} else {
			redoJobs = new HashMap<String, Boolean>();
		}
		
	}

	/**
	 * @throws IOException 
	 */
	public static void stop() throws IOException {
		String path = System.getProperty("datatransfer");
		if (path == null) {
			path = "";
		}
		// 填充日期文件
		File dateFile = new File(path + File.separator + "filedate.dat");
		if (null != dateFile && !dateFile.exists()) {
			dateFile.createNewFile();
		}
		ObjectOutput out = new ObjectOutputStream(
				new FileOutputStream(dateFile));
		out.writeObject(fileDate);
		out.flush();
		out.close();
		// 填充redo标识文件
		File redoFile = new File(path + File.separator + "redojobs.dat");
		if (null != redoFile && !redoFile.exists()) {
			redoFile.createNewFile();
		}
		ObjectOutput redoout = new ObjectOutputStream(new FileOutputStream(
				redoFile));
		redoout.writeObject(redoJobs);
		redoout.flush();
		redoout.close();
	}

	/**
	 * @param file 
	 * @throws IOException 
	 */
	public static void init(String file) throws IOException {
		InputStream in = new BufferedInputStream(new FileInputStream(file));
		properties.load(in);
	}

	/**
	 * 获取配置信息
	 * 
	 * @param orientName 
	 * @return
	 */
	public static String transIt(String orientName) {
		return properties.getProperty(orientName, orientName).trim();
	}

	/**
	 * @param fileName 
	 * @param newDate 
	 * @return
	 */
	public static boolean isNewerThanLast(String fileName, Date newDate) {
		if (fileDate.get(fileName) == null) {
			fileDate.put(fileName, newDate);
			return true;
		}
		Date lastDate = (Date) fileDate.get(fileName);
		if (newDate.after(lastDate)) {
			fileDate.put(fileName, newDate);
			return true;
		}
		return false;
	}

	/**
	 * @return
	 */
	public static synchronized ExecutorService getpool() {
		if (pool == null) {
			pool = Executors.newCachedThreadPool();
		}
		return pool;
	}

	/**
	 * @param jobName 
	 * @param flag 
	 */
	public static synchronized void setJobRedoFlag(String jobName, boolean flag) {
		String key = jobName + sdf.format(new Date());
		redoJobs.clear();
		redoJobs.put(key, Boolean.valueOf(flag));
	}

	/**
	 * @param jobName 
	 * @return 
	 */
	public static synchronized boolean getJobRedoFlag(String jobName) {
		String key = jobName + sdf.format(new Date());
		return redoJobs.get(key) == null ? true : ((Boolean) redoJobs.get(key))
				.booleanValue();
	}
}