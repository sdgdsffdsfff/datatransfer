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

/**
 * @author liubing
 *
 */
public final class FTPClientFactory {
	private static final String FTP = "ftp";
	private static final String SFTP = "sftp";
	
	private FTPClientFactory() {
		
	}
	
	/**
	 * 工厂模式生成FTP客户端
	 * @param ftpType FTP类型
	 * @return 得到一个下载客户端
	 */
	public static BaseFTPClient createFTPClient(String ftpType) {
		BaseFTPClient ftpClient = null;
		if (ftpType.equalsIgnoreCase(FTP)) {
			ftpClient = new CommonFTPClient();
		} else if (ftpType.equalsIgnoreCase(SFTP)) {
			ftpClient = new SFTPClient();
		}
	
		return ftpClient;
	}

}
