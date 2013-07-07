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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.panguso.datatransfer.conf.DownloadFileInfo;
import com.panguso.datatransfer.conf.FtpConf;
import com.panguso.datatransfer.conf.JobConf;
import com.panguso.datatransfer.config.ConfigFactory;
import com.panguso.datatransfer.downloadUtil.DownloadCallSingleConn;


/**
 * @author liubing
 *
 */
public class CommonDownloadService implements CommonService {
	private static final Logger LOG = LoggerFactory
			.getLogger(CommonDownloadService.class);
	
	private final int success = 1;   //下载成功标志
	private final int failed = 0;    //下载失败标志
	
	protected String prefix = "";		//job前缀
	protected String today = "";		//当天时间
	protected String targetday = "";	//目标下载时间
	protected String remoteRootPath = null;		//待下载的远程路径
	protected String localRootPath = null;		//待下载到的本地路径
	protected JobConf jobConf = null;			//JOB配置信息

	@Override
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

//	/**
//	 * 设置JOB前缀
//	 */
//	protected void setPrefix() {
//		setPrefix("");
//	}

	/**
	 * 数据接入工作入口
	 * @return 数据接入是否成功
	 */
	@Override
	public boolean doService() {
		return true;
	}
	
	
	/**
	 * 将待下载的文件列表下载到本地
	 * @param ftpConf ftp下载的相关配置
	 * @param filelist 待下载的文件映射列表
	 * @return
	 */
	protected ArrayList<Boolean> downloadFileCall(FtpConf ftpConf, ArrayList<DownloadFileInfo> filelist) {
		int tried = 0;	//已尝试次数
		//最大尝试次数、多线程个数
		int maxtried = ConfigFactory.getInt("thread_download.max_try", 3);	
		int threadNum = ConfigFactory.getInt("thread_download.download_thread_num", 3);
		//初始化每个下载任务状态为未完成
		for (DownloadFileInfo info : filelist) {
			info.setDownloadres(failed);
		}
		//创建多线程下载
		ExecutorService pool = Executors.newCachedThreadPool();
		while (tried < maxtried) {
			tried++;
			int idx = 0;
			while (idx < filelist.size()) {
				//为每个待下载文件创建一个线程
				List<Future<Boolean>> reslist = new ArrayList<Future<Boolean>>();

				int k = 0;
				boolean hasdata = false;
				while (k < threadNum && idx < filelist.size()) {
					DownloadFileInfo info = filelist.get(idx);
					if (info.getDownloadres() != success) {
						k++;
						hasdata = true;
						ArrayList<DownloadFileInfo> tempjob = new ArrayList<DownloadFileInfo>();
						tempjob.add(info);
						Callable<Boolean> downloadcall = new DownloadCallSingleConn(ftpConf,
								tempjob);
						Future<Boolean> future = pool.submit(downloadcall);
						reslist.add(future);
					}
					idx++;
				}

				if (!hasdata) {
					break;
				}
				for (int j = 0; j < reslist.size(); j++) {
					Future<Boolean> res = reslist.get(j);
					try {
						res.get(1000 * 60 * 10, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {
						LOG.warn("{}", e);
					} catch (ExecutionException e) {
						LOG.warn("{}", e);
					} catch (TimeoutException e) {
						LOG.warn("{}", e);
					}
				}	
			}
		}
		
		pool.shutdown();
		ArrayList<Boolean> res = new ArrayList<Boolean>();
		for (DownloadFileInfo info : filelist) {
			if (success == info.getDownloadres()) {
				res.add(true);
			} else {
				res.add(false);
			}
		}
		return res;
	}
	
}