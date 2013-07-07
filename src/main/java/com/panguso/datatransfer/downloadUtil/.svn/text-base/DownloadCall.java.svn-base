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

import com.panguso.datatransfer.conf.DownloadFileInfo;
import com.panguso.datatransfer.conf.FtpConf;
import com.panguso.datatransfer.ftp.BaseFTPClient;
import com.panguso.datatransfer.ftp.FTPClientFactory;
import com.panguso.datatransfer.util.DateUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liubing
 *
 */
public class DownloadCall implements Callable<Boolean> {
	private static final Logger LOG = LoggerFactory.getLogger(DownloadCall.class);
	private FtpConf fc; 	//待下载的FTP服务器的信息
	private ArrayList<DownloadFileInfo> filelist = null;

	/**
	 * 构造函数
	 * @param fc FTP配置信息
	 * @param filelist	待下载的文件信息列表
	 */
	public DownloadCall(FtpConf fc, ArrayList<DownloadFileInfo> filelist) {
		this.fc = fc;
		this.filelist = filelist;
	}

	@Override
	public Boolean call() throws Exception {
		//根据FTP类型创建FTP下载对象
		BaseFTPClient ftpClient = FTPClientFactory.createFTPClient(fc.getType());
		if (null == ftpClient) {
			LOG.warn("创建FTP客户端失败！");
			return false;
		}
		try {
			//连接FTP服务器
			if (ftpClient.connect(fc.getHost(), fc.getPort(), fc.getUsername(), 
					fc.getPassword(), fc.getEncode())) {
				for (Iterator<DownloadFileInfo> it = filelist.iterator(); it.hasNext();) {
					DownloadFileInfo fi = it.next();
					String remoteFileName = fi.getRemotePath();
					String localFileName = fi.getLocalPath();
					LOG.info("Begin to download ：" + remoteFileName + " to " + localFileName);
					long start = System.currentTimeMillis();
					try {
						//下载文件
						ftpClient.download(remoteFileName, localFileName);
					} catch (Exception e) {
						LOG.error("{}", e);
					}
					long end = System.currentTimeMillis();
					LOG.info("Download " + remoteFileName + " success，total cost："
							+ DateUtil.getReadableTime(end - start));

					it.remove();
				}
			} else {
				LOG.error("connect {}:{} fail", fc.getHost(), fc.getPort());
				return false;
			}
		} catch (Exception e) {
			LOG.error("下载时发生异常{}", e);
			return false;
		} finally {
			if (ftpClient != null) {
				ftpClient.disconnect();
			}
		}
		return true;
	}
}