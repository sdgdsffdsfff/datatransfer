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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;
import com.panguso.datatransfer.util.CommonUtil;

/**
 * @author liubing
 *
 */
public class FtpDownloadConf {
	private static final Logger LOG = LoggerFactory.getLogger(FtpDownloadConf.class); 
	private String prefix = null;
	
	private String remotePath = null;
	private String localPath = null;
	private Boolean isConfigDownloadDir = true;
	private Set<String> downloadDirSet = null;
	private Map<String, HashSet<String>> downloadDirFilesMap = null;
	private String dataFileSuffix = "txt";
	private Map<String, String> fileTableMap = null;
	private Map<String, HashSet<Integer>> fileSkipMap = null;
	private Map<String, Integer> filePrimarykeyIdxMap = null;
	
	/**
	 * 构造函数
	 * @param prefix config文件的XPath前缀
	 */
	public FtpDownloadConf(String prefix) {
		this.prefix = prefix;
	}
	
	
	/**
	 * 初始化方法
	 */
	public void init() {
		
		LOG.info("Begin to init FtpDownloadConf for " + prefix);
		//初始化远程下载路径
		String remotepath = ConfigFactory.getString(prefix + Common.REMOTEPATH);
		if (CommonUtil.isNullOrEmpty(remotepath)) {
			LOG.error("ERROR： init FtpDownloadConf failed，check remotepath tag");
			System.exit(-1);
		}
		this.remotePath = remotepath;
		
		//初始化本地下载路径
		String localpath = ConfigFactory.getString(prefix + Common.LOCALPATH);
		if (CommonUtil.isNullOrEmpty(localpath)) {
			LOG.error("ERROR： init FtpDownloadConf failed，check remotepath tag");
			System.exit(-1);
		}
		this.localPath = localpath;
		
		//初始化数据文件的后缀格式，默认为txt
		String dataSuffix = ConfigFactory.getString(prefix + Common.DATAFILESUFFIX);
		if (CommonUtil.isNullOrEmpty(dataSuffix)) {
			LOG.warn("ERROR： init localPath failed，check dataFileSuffix tag");
			System.exit(-1);
		}
		this.dataFileSuffix = dataSuffix;
		
		//初始化指定下载目录开关
		isConfigDownloadDir = ConfigFactory.getBoolean(prefix + Common.DOWNDIRSWITCH, true);
		if (isConfigDownloadDir) {
			initDownloadDirSet();
			initDownloadDirFilesMap();
		}
		
		//初始化文件名与表名对应
		initFileTableMap();
		
		//初始化文件要忽略的列集合
		initFieldFieldSkip();
		
		//初始化文件字段的主键序号映射集合
		initFilePrimaryKeyIdxMap();
	
		LOG.info("init FtpDownloadConf for " + prefix + " success!");
	}


	/**
	 * FTP下载方式，初始化远程待下载的文件目录
	 */
	private void initDownloadDirSet() {
		String downloadDirTag = prefix + Common.DIR;
		downloadDirSet = CommonUtil.getSetFromTag(downloadDirTag);
		if (null == downloadDirSet) {
			downloadDirSet = new HashSet<String>();
			LOG.warn("WARN: init FtpDownloadConf failed! check dirs tag!");
		}
	}
	

	/**
	 * 初始化下载目录与这个目录下要下载的文件列表的对应关系
	 */
	private void initDownloadDirFilesMap() {
		if (downloadDirFilesMap == null) {
			downloadDirFilesMap = new HashMap<String, HashSet<String>>();
		}
		if (!downloadDirSet.isEmpty()) {
			for (String dir : downloadDirSet) {
				String dirFilesMapTag = prefix + Common.DOT + dir + Common.DOWNLOADFILE;
				Set<String> filesSet = CommonUtil.getSetFromTag(dirFilesMapTag);
				if (null == filesSet) {
					LOG.warn("WARN: init FtpDownloadConf failed! check _downloadfile tag!");
					continue;
				} else {
					downloadDirFilesMap.put(dir, (HashSet<String>) filesSet);
				}
			}
		} else {
			LOG.warn("WARN: init FtpDownloadConf failed! check dirs tag!");
		}
	}
	
	
	
	/**
	 * 初始化数据文件需要忽略（跳过）的字段
	 * 可以为空，即无须忽略字段
	 */
	private void initFieldFieldSkip() {
		if (null == fileSkipMap) {
			fileSkipMap = new HashMap<String, HashSet<Integer>>(); 
		}
		
		String fileSkipMapTag = prefix + Common.FILESKIP;
		HashMap<String, String> skipMap = CommonUtil.getMapFromTag(fileSkipMapTag);
		if (skipMap != null) {
			for (String file : skipMap.keySet()) {
				String fileSkipStr = skipMap.get(file);
				HashSet<Integer> intSet = CommonUtil.getIntSetFromString(fileSkipStr, Common.COMMA);
				if (!CommonUtil.isNullOrEmpty(intSet)) {
					fileSkipMap.put(file, intSet);
				}
			}
		} else {
			LOG.error("init initFieldFieldSkip failed! please check file_skip tag");
			System.exit(-1);
		}
	}
		
	
	/**
	 * 初始化数据表字段映射关系
	 */
	private void initFileTableMap() {
		String fileTableMapTag = prefix + Common.FILETABLEMAP;
		HashMap<String, String> map = CommonUtil.getMapFromTag(fileTableMapTag);
		//这个不能为空，若为空数据接入到哪个表那
		if (!CommonUtil.isNullOrEmpty(map)) {
			fileTableMap = map;
		} else {
			LOG.error("init initFieldFieldSkip failed! please check file_table_map tag");
			System.exit(-1);
		}
	}
	
	
	/**
	 * 初始化文件的字段对应于接入数据表主键的序号 的映射关系
	 * 如book文件第1列数据对应于prod_book数据表的主键，则表示为  book:1
	 */
	private void initFilePrimaryKeyIdxMap() {
		String filePrimarykeyIdxMapTag = prefix + Common.FILEPRIMARYKEY;
		HashMap<String, String> map = CommonUtil.getMapFromTag(filePrimarykeyIdxMapTag);
		if (!CommonUtil.isNullOrEmpty(map)) {
			if (null == filePrimarykeyIdxMap) {
				filePrimarykeyIdxMap = new HashMap<String, Integer>();
				for (String key : map.keySet()) {
					filePrimarykeyIdxMap.put(key, Integer.valueOf(map.get(key)));
				}
			}
		} else {
			LOG.error("init initFieldFieldSkip failed! please check file_primary_field tag");
			System.exit(-1);
		}
	}
	
	public String getRemotePath() {
		return remotePath;
	}

	public String getLocalPath() {
		return localPath;
	}

	public Boolean getIsConfigDownloadDir() {
		return isConfigDownloadDir;
	}

	public Set<String> getDownloadDirSet() {
		return downloadDirSet;
	}

	public Map<String, HashSet<String>> getDownloadDirFilesMap() {
		return downloadDirFilesMap;
	}

	public String getDataFileSuffix() {
		return dataFileSuffix;
	}

	public Map<String, String> getFileTableMap() {
		return fileTableMap;
	}

	public Map<String, HashSet<Integer>> getFileSkipMap() {
		return fileSkipMap;
	}

	public Map<String, Integer> getFilePrimarykeyIdxMap() {
		return filePrimarykeyIdxMap;
	}
}
