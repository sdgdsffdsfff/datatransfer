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
import java.util.ArrayList;
import java.util.Date;
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
import com.panguso.datatransfer.util.IOUtil;

/**
 * @author liubing
 *
 */
public class BigFTPDownloadService extends CommonDownloadService {

	private static final Logger LOG = LoggerFactory.getLogger(BigFTPDownloadService.class);

	private FtpConf ftpConf = null;
	private FtpDownloadConf ftpDownloadConf = null;
	protected String writedownloadfilepath = null;
	private Map<String, HashSet<String>> dirFilesMap = null;
	
	
	/**
	 * 设置要下载的目标时间
	 */
	public void setdaystr() {
		today = DateUtil.DAYSDF.format(new Date());
		targetday = today;
		LOG.info("targetday is {}", targetday);
	}
	

	/**
	 * 	初始化参数
	 */
	protected void init() {
		//获取JobConf
		jobConf = GlobalVariable.getJobConf(prefix);
		if (null == jobConf) {
			LOG.error("get jobConf of {} failed in BigFtpDownloadService:initVariables", prefix);
			System.exit(-1);
		}
		
		//获取FtpConf
		ftpConf = jobConf.getFtpConf();
		if (null == ftpConf) {
			LOG.error("get ftpConf of job {} failed in BigFtpDownloadService:initVariables", jobConf.getJobName());
			System.exit(-1);
		}
		
		//获取FtpDownloadConf
		ftpDownloadConf = ftpConf.getFtpDownloadConf();
		if (null == ftpDownloadConf) {
			LOG.error("get ftpDownloadConf of job {} failed in BigFtpDownloadService:initVariables", jobConf.getJobName());
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
	
	@Override
	public boolean doService() {
		//初始化
		init();
		
		//是否有需要下载的文件的标志
		boolean hasDownloaded = true;
		//获取目录与下载文件映射map，逐个目录开始处理
		for (String dir : ftpDownloadConf.getDownloadDirSet()) {
			Set<String> needDownloadFileSet = dirFilesMap.get(dir);
			if (CommonUtil.isNullOrEmpty(needDownloadFileSet)) {
				LOG.info("No files need to be downloaded in the dir : " + dir + " of job: " + jobConf.getJobName());
				continue;
			}
			//创建本地目录
			String subDir = targetday + File.separator + dir + File.separator;
			FileUtil.createLocalPath(localRootPath, subDir);
			
			//获取要下载的文件列表
			List<String> availFilesList = getAvailFiles(targetday, dir, needDownloadFileSet);
			if (CommonUtil.isNullOrEmpty(availFilesList)) {
				LOG.info("No file need download from dir {}", dir);
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
			
			//下载目录中的各个待下载文件，判断下载结果
			ArrayList<Boolean> res = downloadFileCall(ftpConf, downloadFileList);
			for (int idx = 0; idx < res.size(); idx++) {
				if (!res.get(idx)) {
					LOG.error("download " + downloadFileList.get(idx).getRemotePath() + " failed");
					hasDownloaded = false;
				}
			}
			
			//将下载后的各个文件名称写入本地文件
			IOUtil.writeToFile(writedownloadfilepath, downloadFileList);
		}
		
		if (hasDownloaded) {
			LOG.info("Download process of {} job have finished!", jobConf.getJobName());
		} else {
			//下载失败，删除记录下载记录的文件以告知导入类
			FileUtil.removeFile(writedownloadfilepath);
			LOG.error("Download process of {} job failed!", jobConf.getJobName());
		}

		return hasDownloaded;
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
				
				//获取目标日期该目录下的文件列表
				String subDir = targetDay + File.separator + dir + File.separator;
				ArrayList<String> fileArray = ftpClient.listNames(remotePath + subDir);
				if (null == fileArray || fileArray.isEmpty()) {
					LOG.error("There are no files in FTP path: " + remotePath + subDir);
					return null;
				}
				
				StringBuilder tmpsb = new StringBuilder();
				for (String ftpFile : fileArray) {
					tmpsb.append(ftpFile).append(",");
				}
				LOG.debug("list files:{}", tmpsb.toString());
				
				//判断done文件是否存在
				String targetDoneFile = targetday + Common.DOT + Common.DONEFILESUFFIX;
				boolean containDoneFile = false;
				for (String ftpFile : fileArray) {
					if (ftpFile.equalsIgnoreCase(targetDoneFile)) {
						containDoneFile = true;
						break;
					}
				}
				if (!containDoneFile) {
					LOG.error("There are no done file in the DIR " + remotePath);
					return null;
				}
				
				//检查每个待下载文件的md5文件和数据文件是否存在
				HashSet<String> dataFileSet = new HashSet<String>();
				HashSet<String> md5FileSet = new HashSet<String>();
				for (String ftpFile : fileArray) {
					//如果是done文件，则继续
					if (ftpFile.equalsIgnoreCase(targetDoneFile)) {
						continue;
					}
					
					//检查是否是全量数据文件，通过检查时间格式
					String dataOfFile = FileUtil.getTimeFromFileName(ftpFile);
					if (dataOfFile != null && !dataOfFile.matches(Common.REGEXDAY)) {
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
					
					if (md5FileSet.contains(md5File)) {
						availFiles.add(noSuffixfileName);
					}
				}
				
				if (availFiles.size() < needDownloadFileList.size()) {
					LOG.error("number of files checked success is less than real need to download file number！");
					return null;
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

}
