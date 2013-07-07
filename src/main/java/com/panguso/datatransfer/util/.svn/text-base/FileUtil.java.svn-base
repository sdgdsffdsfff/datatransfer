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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.conf.JobConf;
import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;

/**
 * @author liubing
 *
 */
public final class FileUtil {
	private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);
	/**
	 * 文件名的分隔符
	 */
	public static final String SEPARATOR = "_";
	
	private FileUtil() {
		
	}
	
	/**
	 * @function 从文件名中提取时间，文件名格式如：20120514_androidGame_1.txt
	 * @param fileName 待提取时间的文件名
	 * @return 文件名中的时间部分，如本例中的 20120514
	 */
	public static String getTimeFromFileName(String fileName) {
		//检查文件名是否为空
		if (CommonUtil.isNullOrEmpty(fileName)) {
			LOG.warn("file name is null or empty,cannot extract time from it!");
			return null;
		}
		//检查格式是否符合要求
		String[] segments = fileName.split(SEPARATOR);
		if (CommonUtil.isNullOrEmpty(segments) 
				|| segments.length < 3) {
			LOG.warn("{} is not conform to naming style, e.g. 20120514_androidGame_1.txt", fileName);
			return null;
		}
		return segments[0];
	}	
	
	/**
	 * @function 获取文件的短文件名，文件名格式如：20120514_androidGame_1.txt
	 * @param fileName 带操作的文件名
	 * @return 文件名中间的部分，如本例中的 androidGame
	 */
	public static String getShortFileName(String fileName) {
		//检查文件名是否为空
		if (CommonUtil.isNullOrEmpty(fileName)) {
			LOG.warn("file name is null or empty,cannot short name from it!");
			return null;
		}
	
		File file = new File(fileName);
		fileName = file.getName();
		
		//检查格式是否符合要求
		String[] segments = fileName.split(SEPARATOR);
		if (CommonUtil.isNullOrEmpty(segments) 
				|| segments.length < 3) {
			LOG.warn("{} is not conform to naming style, e.g. 20120514_androidGame_1.txt", fileName);
			return null;
		}
		return segments[1];
	}
	

	/**
	 * @function 从文件名中提取截取的文件名，文件名格式如：20120514_androidGame_1.txt
	 * @param fileName 待提取的文件名
	 * @return 文件名中的序号，如本例中的 1
	 */
	public static int getIndexFromFileName(String fileName) {
		//检查文件名是否为空
		if (CommonUtil.isNullOrEmpty(fileName)) {
			LOG.warn("file name is null or empty,cannot short name from it!");
			return -1;
		}
		//检查格式是否符合要求
		String separator = "\\.";
		String[] segs = fileName.split(separator);
		if (CommonUtil.isNullOrEmpty(segs) || segs.length < 2) {
			LOG.warn("{} is not conform to naming style, e.g. 20120514_androidGame_1.txt", fileName);
			return -1;
		}
		fileName = segs[0];
		
		String[] segments = fileName.split(SEPARATOR);
		if (CommonUtil.isNullOrEmpty(segments) 
				|| segments.length < 3 || !segments[2].matches(Common.REGEXNUMBER)) {
			LOG.warn("{} is not conform to naming style, e.g. 20120514_androidGame_1.txt", fileName);
			return -1;
		}
		return Integer.valueOf(segments[2]);
	}
	

	/**
	 * @function 从文件名中提取除去后缀的文件名，文件名格式如：20120514_androidGame_1.txt
	 * @param fileName 待处理的文件名
	 * @return 去掉后缀的文件名， 如本例中 20120514_androidGame_1
	 */
	public static String removeSuffix(String fileName) {
		//检查文件名是否为空
		if (CommonUtil.isNullOrEmpty(fileName)) {
			LOG.warn("file name is null or empty, cannot remove suffix for it");
			return null;
		}
		//检查格式是否符合要求
		if (!fileName.contains(Common.DOT)) {
			return null;
		}
		
		int dotIdx = fileName.lastIndexOf(Common.DOT);
		return fileName.substring(0, dotIdx);
	}


	/**
	 * @function 删除filePath指向的文件
	 * @param filePath 
	 */
	public static void removeFile(String filePath) {
		if (CommonUtil.isNullOrEmpty(filePath)) {
			LOG.debug("path of file is null or empty!");
			return;
		}
		File file = new File(filePath);
		if (file.exists()) {
			file.delete();
		}
	}
	

	/**
	 * @function 检查filePath对应文件是否存在
	 * @param filePath 待检查的文件路径
	 */
	public static boolean isFileExist(String filePath) {
		if (CommonUtil.isNullOrEmpty(filePath)) {
			LOG.debug("path of file is null or empty!");
			return false;
		}
		File file = new File(filePath);
		return file.exists();
	}
	
	/**
	 * @function 在本地创建目录
	 * @param path 根目录
	 * @param subDir 子目录
	 */
	public static void createLocalPath(String path, String subDir) {
		if (CommonUtil.isNullOrEmpty(path) || CommonUtil.isNullOrEmpty(subDir)) {
			return;
		}
		if (!path.endsWith(File.separator)) {
			path += File.separator;
		}
		path = path + subDir;
		File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}
		LOG.debug("create the diretory of {} success!", path);
	}
	
	/**
	 * 去除job名称的增量、全量前缀
	 * @param jobName job名称
	 * @return 去除增量、全量前缀的段名称
	 */
	public static String getShortJobName(String jobName) {
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName is null or empty in FileUtil.getShortJobName method");
			return null;
		}
		String shortName = jobName;
		if (jobName.startsWith(Common.BIG)) {
			shortName = jobName.substring(Common.BIG.length());
		} else if (jobName.startsWith(Common.DY)) {
			shortName = jobName.substring(Common.DY.length());
		}
		return shortName;
	}
	
	
	/**
	 * @param fileName 待创建lock文件的job名称
	 * @return 需要创建的lock文件路径
	 */
	public static String getLockFilePath(String fileName) {
		String path = "";
		String rootPath = ConfigFactory.getString(Common.CONFIGPATH);
		if (CommonUtil.isNullOrEmpty(rootPath) || CommonUtil.isNullOrEmpty(fileName)) {
			LOG.error("paraments is invalid in FileUtil:getLockFilePath method!");
			return path;
		}
		
		if (!rootPath.endsWith(File.separator)) {
			rootPath += File.separator;
		}
		
		fileName += Common.LOCK;
		return rootPath + "conf" + File.separator + fileName;
	}
	
	
	/**
	 * @param fileName 待创建done文件的job名称
	 * @return 需要创建的done文件路径
	 */
	public static String getDoneFilePath(String fileName) {
		String path = "";
		String rootPath = ConfigFactory.getString(Common.CONFIGPATH);
		if (CommonUtil.isNullOrEmpty(rootPath) || CommonUtil.isNullOrEmpty(fileName)) {
			LOG.error("paraments is invalid in FileUtil:getDoneFilePath method!");
			return path;
		}
		
		if (!rootPath.endsWith(File.separator)) {
			rootPath += File.separator;
		}
		
		fileName += Common.DONE;
		return rootPath + "conf" + File.separator + fileName;
	}
	
	/**
	 * @param fileName 待创建doing文件的job名称
	 * @return 需要创建的doing文件路径
	 */
	public static String getDoingFilePath(String fileName) {
		String path = "";
		String rootPath = ConfigFactory.getString(Common.CONFIGPATH);
		if (CommonUtil.isNullOrEmpty(rootPath) || CommonUtil.isNullOrEmpty(fileName)) {
			LOG.error("paraments is invalid in FileUtil:getDoneFilePath method!");
			return path;
		}
		
		if (!rootPath.endsWith(File.separator)) {
			rootPath += File.separator;
		}
		
		fileName += Common.DOING;
		return rootPath + "conf" + File.separator + fileName;
	}
	
	
	/**
	 * 创建文件，如果已经存在则删除重新生成
	 * @param filePath 带创建文件的路径
	 * @return 创建结果
	 */
	public static boolean createFile(String filePath) {
		if (CommonUtil.isNullOrEmpty(filePath)) {
			LOG.error("create file failed!, file path is null or empty!");
			return false;
		}
		removeFile(filePath);
		File file = new File(filePath);
		try {
			file.createNewFile();
		} catch (IOException e) {
			LOG.error("create file: {}  error: {}", filePath, e.toString());
		}
		return true;
	}
	
	/**
	 * 注意：相同名称的增量和全量两个job，只通过段名称创建一个lock文件
	 * 因为增量全量都作为一个整体互斥的执行
	 * @param jobNames 待加锁的Job名称数组
	 */
	public static void createLockFile(String[] jobNames) {
		if (CommonUtil.isNullOrEmpty(jobNames)) {
			LOG.error("cannot create lock file for {}, it is null or empty!", jobNames);
			System.exit(-1);
		}
		//获取job的短名称集合
		Set<String> uniJobNameSet = new HashSet<String>();
		for (String jobName : jobNames) {
			String shortName = getShortJobName(jobName);
			if (shortName != null) {
				uniJobNameSet.add(shortName);
			}
		}
		//为每个job生成一个lock文件
		for (String jobName : uniJobNameSet) {
			String lockFilePath = getLockFilePath(jobName);
			LOG.debug("create lock file for {}", lockFilePath);
			if (!createFile(lockFilePath)) {
				LOG.error("create lock file for {} error", jobName);
			}
		}
	}
	
		
	/**
	 * 数据接入job完成以后，为其创建done文件，表示该job已经完成
	 * 注意：done文件时每个job对应一个，增量全量互不干扰
	 * @param jobName 待创建done文件的Job名称
	 */
	public static void createDoneFile(String jobName) {
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName on which to create done file is null or empty!");
			System.exit(-1);
		}
		
		//获取done文件的路径
		String doneFilePath = getDoneFilePath(jobName);
		//创建done文件
		LOG.debug("create done file for {}", doneFilePath);
		if (!createFile(doneFilePath)) {
			LOG.error("create done file for {} error! ", jobName);
			System.exit(-1);
		}
	}
	
	/**
	 * 为job创建doing文件，表示本job正在进行
	 * @param jobName 待创建doing文件的job名称
	 */
	public static void createDoingFile(String jobName) {
		if (CommonUtil.isNullOrEmpty(jobName)) {
			LOG.error("jobName is null or empty, cannot create doing file for it!");
			System.exit(-1);
		}
		
		//获取done文件的路径
		String doingFilePath = getDoingFilePath(jobName);
		//创建done文件
		LOG.debug("create doing file for {}", doingFilePath);
		if (!createFile(doingFilePath)) {
			LOG.error("create done file for {} error! ", jobName);
			System.exit(-1);
		}
	}
	
	/**
	 * 注意：done文件时每个job对应一个，增量全量互不干扰
	 * @param jobConf 待创建done的job配置对象
	 */
	public static void createDoneFile(JobConf jobConf) {
		//获取job名称
		if (null == jobConf) {
			LOG.error("create done file for error, JobConf is null!");
			System.exit(-1);
		}
		String jobName = jobConf.getJobName();
		FileUtil.createDoneFile(jobName);
	}
	
	/**
	 * @param argv 
	 */
	public static void main(String[] argv) {
		
		Map<String, String> map = new HashMap<String, String>();
		map.put("key_1", "value_1");
		map.put("key_2", "value_2");
		map.put("key_3", "value_3");
		map.put("key_4", "value_4");
		
		for (String value : map.values()) {
			LOG.info("value: " + value);
		}
		
//		String[] lockFiles = new String[]{"read", "mm", "wxcs", "bigmm", "dymm"};
//		createLockFile(lockFiles);
		
//		String name = "20120514_androidGame_1.txt";
//		String[] segs = name.split("\\.");
//		System.out.println(segs[0]);
//		System.out.println(getIndexFromFileName(name));
		
		
//		String path = "d:/hello/hello.txt";
//		String path2 = "C:\\d_hello.txt";
//		File file = new File(path);
//		String fileName = file.getName();
//		String parent = file.getParentFile().getName();
//		
//		System.out.println(parent + " " + fileName);
//		List<String> list = new ArrayList<String>();
//		list.add("20130419_read_0.txt");
//		list.add("20130409_read_1.txt");
//		list.add("20130409_read_1.md5");
//		list.add("20130409_read_0.md5");
//		
//		for (String file : list) {
//			System.out.println(file);
//		}
//		
//		Collections.sort(list, null);
//		
//		for (String file : list) {
//			System.out.println(file);
//		}
	}
}
