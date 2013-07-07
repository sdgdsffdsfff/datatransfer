/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.downloadUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.panguso.datatransfer.conf.DownloadFileInfo;
import com.panguso.datatransfer.conf.FtpConf;
import com.panguso.datatransfer.ftp.BaseFTPClient;
import com.panguso.datatransfer.ftp.FTPClientFactory;
import com.panguso.datatransfer.util.DateUtil;
import com.panguso.datatransfer.util.FileUtil;


/**
 * @author zhaopeng
 * 每次下载都单独连接
 * 如果多个文件里有一个下载失败，则待全部下载完毕后返回false
 */
public class DownloadCallSingleConn implements Callable<Boolean> {
	private Logger log = LoggerFactory.getLogger(DownloadCallSingleConn.class);
	private final int success = 1;
	private final int failed = -1;
	
	private ArrayList<DownloadFileInfo> filelist = null;
	private FtpConf ftpConf;
	
	/**
	 * @param fc FTP配置
	 * @param fileList 待下载文件列表
	 */
	public DownloadCallSingleConn(FtpConf fc,
			ArrayList<DownloadFileInfo> fileList) {
		this.ftpConf = fc;
		this.filelist = fileList;
	}

	@Override
	public Boolean call() throws Exception {		
		BaseFTPClient ftpClient = FTPClientFactory.createFTPClient(ftpConf.getType());
		if (null == ftpClient) {
			log.error("create ftp client failed!");
			return false;
		}
		
		boolean res = true;
		if (ftpClient.connect(ftpConf.getHost(), ftpConf.getPort(), ftpConf.getUsername(), 
				ftpConf.getPassword(), ftpConf.getEncode())) {
			for (Iterator<DownloadFileInfo> it = filelist.iterator(); it.hasNext();) {
				DownloadFileInfo fi = it.next();
				String remoteFileName = fi.getRemotePath();
				String localFileName = fi.getLocalPath();
				this.log.info("Begin to download file " + remoteFileName + " to " + localFileName);
				long start = System.currentTimeMillis();
				try {
					FileUtil.removeFile(localFileName);
					ftpClient.download(remoteFileName, localFileName);
					fi.setDownloadres(success);
					
					long end = System.currentTimeMillis();
					log.info("download file " + remoteFileName + "success，cost：" + DateUtil.getReadableTime(end - start));
					
				} catch (Exception e) {
					log.info("download file " + remoteFileName + "failed!，please check error log!");
					log.error("{}", e);
					fi.setDownloadres(failed);
					res = false;
				}
			}
		}
		return res;
	}

}
