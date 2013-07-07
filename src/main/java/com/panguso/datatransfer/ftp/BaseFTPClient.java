/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.ftp;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author zhaopeng
 *
 */
public class BaseFTPClient {

	/**
	 * 建立于FTP服务器连接
	 * @param hostname FTP服务器主机
	 * @param port	端口
	 * @param username	用户名
	 * @param password	密码
	 * @param encode	编码	
	 * @return 返回连接是否成功
	 * @throws Exception	异常
	 */
	public boolean connect(String hostname, int port, String username,
			String password, String encode) throws Exception {
		return true;
	}
	
	/**
	 * 断开连接
	 * @throws IOException 异常
	 */
	public void disconnect() throws IOException {
	}
	
	/**
	 * 从FTP服务器上下载文件到本地
	 * @param remote	待下载文件在FTP服务器上的路径
	 * @param local		要下载到本地的路径
	 * @throws Exception	异常
	 */
	public void download(String remote, String local) throws Exception {
		return;
	}
	
	/**
	 * 向FTP服务器上上传文件
	 * @param local		待上传的文件在本地的路径
	 * @param remote	要上传到FTP服务器上的路径
	 * @throws Exception	异常
	 */
	public void upload(String local, String remote) throws Exception {
	}
	
	
	/**
	 * 列出FTP服务器上某一路径下的所有文件
	 * @param remotePath	待列出文件的目录
	 * @return	返回列出的文件列表
	 * @throws Exception	异常
	 */
	public ArrayList<String> listNames(String remotePath) throws Exception {
		return null;
	}

}
