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

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.conf.DownloadFileInfo;
import com.panguso.datatransfer.conf.FtpConf;
import com.panguso.datatransfer.conf.FtpDownloadConf;
import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.GlobalVariable;
import com.panguso.datatransfer.ftp.BaseFTPClient;
import com.panguso.datatransfer.ftp.FTPClientFactory;
import com.panguso.datatransfer.util.CommonUtil;
import com.panguso.datatransfer.util.DateUtil;
import com.panguso.datatransfer.util.FileUtil;

/**
 * @author liubing
 *
 */
public class DyFTPDownloadService extends CommonDownloadService {

	private static final Logger LOG = LoggerFactory.getLogger(DyFTPDownloadService.class);

	private FtpConf ftpConf = null;
	private FtpDownloadConf ftpDownloadConf = null;
	private Map<String, HashSet<String>> dirFilesMap = null;	//待下载的目录与其下文件列表的映射
	protected String writedownloadfilepath = null;   			//将下载列表写入文件
		
	/**
	 * 初始化参数
	 */
	protected void initVariables() {
		jobConf = GlobalVariable.getJobConf(prefix);
		if (null == jobConf) {
			LOG.error("get jobConf of " + prefix + " failed in DyFtpDownloadService:initVariables");
			System.exit(-1);
		}
		
		ftpConf = jobConf.getFtpConf();
		if (null == ftpConf) {
			LOG.error("get ftpConf of job {} failed in DyFtpDownloadService:initVariables", jobConf.getJobName());
			System.exit(-1);
		}
		
		ftpDownloadConf = ftpConf.getFtpDownloadConf();
		if (null == ftpDownloadConf) {
			LOG.error("get ftpDownloadConf of job {} failed in DyFtpDownloadService:initVariables", jobConf.getJobName());
			System.exit(-1);
		}
		//获取并检查本地路径与FTP下载路径的格式
		remoteRootPath = ftpDownloadConf.getRemotePath();
		localRootPath = ftpDownloadConf.getLocalPath();
		if (!remoteRootPath.endsWith(File.separator)) {
			remoteRootPath += File.separator;
		}
		if (!localRootPath.endsWith(File.separator)) {
			localRootPath += File.separator;
		}
		LOG.debug("remoteRootPath : " + remoteRootPath);
		LOG.debug("localRootPath : " + localRootPath);		
		//初始化要写入下载列表的文件句柄
		writedownloadfilepath = localRootPath + prefix + Common.DOWNLOADFILE;
		FileUtil.createFile(writedownloadfilepath);
		
		//获取目录与下载文件映射map
		dirFilesMap = ftpDownloadConf.getDownloadDirFilesMap();
		
		//设置要下载的日期, 下载以当天日期为文件名的文件夹下的数据
		setdaystr();
	}
	

	/**
	 * 设置要下载的目标时间
	 */
	public void setdaystr() {
		//获取每个目录下待下载文件对应的上次下载时间
		today = DateUtil.getToday();
		String earliestStamp = today;
		Map<String, String> fileLastStampMap = getLastStampMap(dirFilesMap, earliestStamp);
		if (CommonUtil.isNullOrEmpty(fileLastStampMap)) {
			LOG.error("when get file last stamp map error!!");
		}
		
		targetday = earliestStamp;
		LOG.info("targetday is " + targetday);
	}
	
	@Override
	public boolean doService() {
		//初始化
		initVariables();
		
		boolean hasDownloaded = true;
		//逐个目录开始处理
		for (String dir : ftpDownloadConf.getDownloadDirSet()) {
			Set<String> needDownloadFileSet = dirFilesMap.get(dir);
			if (CommonUtil.isNullOrEmpty(needDownloadFileSet)) {
				LOG.info("There are no files need to be downloaded in the dir : " + dir);
				continue;
			}
			
			//创建本地目录
			while (targetday.compareTo(today) <= 0) {
				LOG.info("Begin to download files of {} of job {}", targetday, jobConf.getJobName());
				//从上次完成到今天的时间的每一天逐个处理
				String subDir = targetday + File.separator + dir + File.separator;
				FileUtil.createLocalPath(localRootPath, subDir);
				
				//获取要下载的文件列表
				List<String> availFilesList = getAvailFiles(targetday, dir, needDownloadFileSet);
				if (CommonUtil.isNullOrEmpty(availFilesList)) {
					LOG.info("no file need downloads from dir {}", dir);
					targetday = DateUtil.addOneDay(targetday);
					continue;
				}
				
				//构造本地路径与远程路径的映射关系
				ArrayList<DownloadFileInfo> downloadFileList = new ArrayList<DownloadFileInfo>();
				for (String tmpFile : availFilesList) {
					//判断该文件是否以当天时间为命名   争取的文件命名规范是：20130411_read_0.txt
					if (!tmpFile.startsWith(targetday)) {
						LOG.warn(tmpFile + " is not conform to naming style, it should like 20130411_read_0.txt");
						continue;
					}
					
					//添加数据文件下载映射
					DownloadFileInfo dataFileInfo = new DownloadFileInfo();
					String dataFile = subDir + tmpFile + Common.DOT + ftpDownloadConf.getDataFileSuffix();
					dataFileInfo.setRemotePath(remoteRootPath + dataFile);
					dataFileInfo.setLocalPath(localRootPath + dataFile);
					downloadFileList.add(dataFileInfo);
					
					//添加MD5文件下载映射
					DownloadFileInfo md5FileInfo = new DownloadFileInfo();
					String md5File = subDir + tmpFile + Common.DOT + Common.MD5SUFFIX;
					md5FileInfo.setRemotePath(remoteRootPath + md5File);
					md5FileInfo.setLocalPath(localRootPath + md5File);
					downloadFileList.add(md5FileInfo);
				}
				
				//下载各文件
				ArrayList<Boolean> res = downloadFileCall(ftpConf, downloadFileList);
				for (int idx = 0; idx < res.size(); idx++) {
					if (!res.get(idx)) {
						LOG.error("download " + downloadFileList.get(idx).getRemotePath() + " failed. return");
						hasDownloaded = false;
					}
				}
				
				//有文件下载失败，则跳出while循环，不再继续下载本目录下其他时间目录
				if (!hasDownloaded) {
					break;
				}
				
				//将下载后的各个文件名称写入本地文件
				writeDownloadInfo(writedownloadfilepath, downloadFileList);
				//开始处理后面一天
				targetday = DateUtil.addOneDay(targetday);
			}
			
			//有文件下载失败，则跳出for循环，不再继续下载其他目录
			if (!hasDownloaded) {
				break;
			}
		}
		
		//判断下载是否全部成功
		if (hasDownloaded) {
			LOG.info("Download process of {} job have finished!", jobConf.getJobName());
		} else {
			//下载失败，删除记录下载记录项的文件以告知导入类
			FileUtil.removeFile(writedownloadfilepath); 
			LOG.error("Download process of {} job failed!", jobConf.getJobName());
		}

		return hasDownloaded;
	}


	//将下载列表downloadFileList的本地路径写入path路径指定的文件中
	private boolean writeDownloadInfo(String path, List<DownloadFileInfo> downloadFileList) {
		boolean writeFlag = true;
		if (CommonUtil.isNullOrEmpty(path)) {
			LOG.error("the file path is null or empty of in DyFtpDownloadService:writeDownloadInfo job {}", jobConf.getJobName());
			return false;
		}
		
		if (CommonUtil.isNullOrEmpty(downloadFileList)) {
			LOG.error("downloadFileList is null or empty in DyFtpDownloadService:writeDownloadInfo job {}", jobConf.getJobName());
			return false;
		}
		
		File file = new File(path);
		FileWriter fWriter = null;
		try {	
			fWriter = new FileWriter(file, true);
			for (DownloadFileInfo downloadInfo : downloadFileList) {
				fWriter.write(downloadInfo.getLocalPath() + System.getProperty("line.separator"));
			}
			fWriter.close();
		} catch (Exception e) {
			LOG.error("{}", e);
		} finally {
			try {
				fWriter.close();
			} catch (Exception e) {
				LOG.error("close file error:{}", e);
			}
		}
		
		return writeFlag;
	}

	
	//获取下载目录下每个文件上次存储的下载时间的映射map
	private Map<String, String> getLastStampMap(Map<String, HashSet<String>> dirToFilesMap, String earliestDate) {
		if (CommonUtil.isNullOrEmpty(dirToFilesMap)) {
			LOG.error("dirFilesMap is null or empty!");
			return null;
		}
		if (CommonUtil.isNullOrEmpty(earliestDate)) {
			earliestDate = DateUtil.getToday();
		}
		
		Map<String, String> lastStampMap = new HashMap<String, String>();
		for (String dir : dirToFilesMap.keySet()) {
			Set<String> fileSet = dirToFilesMap.get(dir);
			if (CommonUtil.isNullOrEmpty(fileSet)) {
				continue;
			}
			for (String fileName : fileSet) {
				String stampKey = dir + FileUtil.SEPARATOR + fileName + FileUtil.SEPARATOR + Common.STAMP;
				String stamp = Common.getLaststamp(stampKey);
				if (stamp.compareTo(earliestDate) < 0) {
					earliestDate = stamp;
				}
				lastStampMap.put(stampKey, Common.getLaststamp(stampKey));
			}
		}
		return lastStampMap;
	}
	

	/**
	 * @function 验证要下载的文件是否已经都存在，返回要下载的文件列表，注意返回的列表不包含文件后缀
	 * @param targetDay 待检查的目标日期
	 * @param dir	待检查的目录
	 * @param needDownloadFileList	需要下载的文件列表
	 * @return
	 */
	protected List<String> getAvailFiles(String targetDay, String dir, Set<String> needDownloadFileList) {
		//根据本job的下载类型获取FTP连接客户端
		BaseFTPClient ftpClient = null;	
		ftpClient = FTPClientFactory.createFTPClient(ftpConf.getType());
		if (null == ftpClient) {
			LOG.error("get ftpClient failed！！!");
			return null;
		}
		
		List<String> availFiles = new ArrayList<String>();
		try {
			//与FTP服务器建立连接
			if (ftpClient.connect(ftpConf.getHost(), ftpConf.getPort(), ftpConf.getUsername(), 
					ftpConf.getPassword(), ftpConf.getEncode())) {
				LOG.info("connect ftp server " + ftpConf.getHost() + " success!");
				
				FtpDownloadConf downloadConf = ftpConf.getFtpDownloadConf();
				String remotePath = downloadConf.getRemotePath();
				if (!remotePath.endsWith(File.separator)) {
					remotePath += File.separator;
				}
				
				remotePath += (targetDay + File.separator + dir + File.separator);
				
				//获取该目录下的文件列表
				ArrayList<String> fileArray = ftpClient.listNames(remotePath);
				if (null == fileArray || fileArray.isEmpty()) {
					LOG.error("There are no files in FTP path: " + remotePath);
					return null;
				}
				
				StringBuilder tmpsb = new StringBuilder();
				for (String ftpFile : fileArray) {
					tmpsb.append(ftpFile).append(",");
				}
				LOG.debug("list files:{}", tmpsb.toString());
				
				//增量数据无需判断done文件是否存在
				
				//检查每个待下载文件的md5文件和数据文件是否存在
				HashSet<String> dataFileSet = new HashSet<String>();
				HashSet<String> md5FileSet = new HashSet<String>();
				for (String ftpFile : fileArray) {

					//检查是否是全量数据文件，通过检查时间格式
					String dataOfFile = FileUtil.getTimeFromFileName(ftpFile);
					if (dataOfFile != null && !dataOfFile.matches(Common.REGEXTIME)) {
						continue;
					}
					
					String fileName = FileUtil.getShortFileName(ftpFile);
					if (needDownloadFileList.contains(fileName)) {
						if (ftpFile.endsWith(downloadConf.getDataFileSuffix())) {
							dataFileSet.add(ftpFile);
						} else if (ftpFile.endsWith(Common.MD5SUFFIX)) {
							md5FileSet.add(ftpFile);
						}
					}
				}
				
				for (String dataFile : dataFileSet) {
					String noSuffixfileName = FileUtil.removeSuffix(dataFile);
					if (CommonUtil.isNullOrEmpty(noSuffixfileName)) {
						break;
					}
					String md5File = noSuffixfileName + Common.DOT + Common.MD5SUFFIX;
					
					if (md5FileSet.contains(md5File) && checkStamp(dir, dataFile)) {
						availFiles.add(noSuffixfileName);
					}
				}
			} else {
				LOG.info("connect ftp server failed!!!");
			}
		} catch (Exception e) {
			LOG.error("{}", e);
		} finally {
			if (ftpClient != null) {
				try {
					ftpClient.disconnect();
				} catch (Exception e) {
					LOG.error("Error when closing ftp link {}", e);
				}
			}
		}
		return availFiles;
	}	
	
	
	/*
		检查要下载的dir目录下的fileName文件是否比上次保存时间有所更新 
		文件名格式如：201304091320_read_1.txt
		如果时间戳大则是更新的文件，如果时间戳相等，则比较文件序号index
	 */
	private boolean checkStamp(String dir, String fileName) {
		if (CommonUtil.isNullOrEmpty(dir) || CommonUtil.isNullOrEmpty(fileName)) {
			return false;
		}
		
		String shortFileName = FileUtil.getShortFileName(fileName);
		String fileStamp = FileUtil.getTimeFromFileName(fileName);
		
		String stampKey = dir + FileUtil.SEPARATOR + shortFileName + FileUtil.SEPARATOR + Common.STAMP;
		String lastStamp = Common.getLaststamp(stampKey);
		

		if (fileStamp.compareTo(lastStamp) > 0) {
			return true;
		} else if (fileStamp.compareTo(lastStamp) == 0) {
			int fileIndex = FileUtil.getIndexFromFileName(fileName);
			String indexKey = dir + FileUtil.SEPARATOR + shortFileName + FileUtil.SEPARATOR + Common.INDEX;
			int lastIndex = Integer.valueOf(Common.getLaststamp(indexKey));
			return fileIndex > lastIndex;
		}
		return false;
	}

}
