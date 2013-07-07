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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.config.ConfigFactory;

/**
 * @author liubing
 *
 */
public class CommonFTPClient extends BaseFTPClient {

	private Logger log = LoggerFactory.getLogger(CommonFTPClient.class);
	private FTPClient ftpClient = new FTPClient();
	private String encode = "UTF-8";

	/**
	 * 构造函数
	 */
	public CommonFTPClient() {
		if (ConfigFactory.getBoolean("common.debug")) {
			this.ftpClient.addProtocolCommandListener(new PrintCommandListener(
					new PrintWriter(System.out)));
		}
	}


	@Override
	public boolean connect(String hostname, int port, String username,
			String password, String encodeStr) throws Exception {
		this.encode = encodeStr;
		ftpClient.connect(hostname, port);
		ftpClient.setControlEncoding(encodeStr);
		ftpClient.setDefaultTimeout(ConfigFactory.getInt(
				"common.conectiontimeout", 60000));
		ftpClient.setConnectTimeout(ConfigFactory.getInt(
				"common.conectiontimeout", 60000));
		ftpClient.setSoTimeout(ConfigFactory.getInt("common.conectiontimeout",
				60000));
		ftpClient.setDataTimeout(ConfigFactory.getInt(
				"common.conectiontimeout", 60000));
		ftpClient.connect(hostname, port);
		if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
			if (ftpClient.login(username, password)) {
				return true;
			}
		}
		disconnect();
		return false;
	}

	@Override
	public void download(String remote, String local) throws IOException {

		boolean isSupportBroke = false;

		// 设置主动模式
		// ftpClient.enterLocalActiveMode();
		ftpClient.enterLocalPassiveMode();
		// 设置以二进制方式传输
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		// 结果状态
		int ftpStatus = 0;
		// 检查远程文件是否存在
		FTPFile[] files = ftpClient.listFiles(new String(remote
				.getBytes(encode), encode));
		if (files.length != 1) {
			log.error("ftp下载路径下数据文件不存在:" + remote);
			// return FTPStatus.Remote_File_Noexist;
			throw new IOException("not found ftpfile:" + remote);
		}

		long remoteSize = files[0].getSize();
		File f = new File(local);
		// 本地文件存在，进行断点下载
		if (isSupportBroke && f.exists()) {
			long localSize = f.length();
			// 判断本地文件大小是否大于远程文件大小
			if (localSize >= remoteSize) {
				log.error("本地文件大于远程文件，下载中止:" + remote + files[0].getName());
				// return FTPStatus.Local_Bigger_Remote;
				return;
			}
			// 进行断点续传，并记录状态
			FileOutputStream out = new FileOutputStream(f, true);
			ftpClient.setRestartOffset(localSize);
			InputStream in = ftpClient.retrieveFileStream(new String(remote
					.getBytes(encode), encode));
			byte[] bytes = new byte[1024];
			int c;
			while ((c = in.read(bytes)) != -1) {
				out.write(bytes, 0, c);
			}
			in.close();
			out.close();
			boolean isDone = ftpClient.completePendingCommand();
			if (isDone) {
				ftpStatus = FTPStatus.DOWNLOAD_FROM_BREAK_SUCCESS;
			} else {
				ftpStatus = FTPStatus.DOWNLOAD_FROM_BREAK_FAILED;
			}
		} else {
			// 不支持断点续传，先删除已经存在的本地文件
			if (f.exists()) {
				f.delete();
			}
			OutputStream out = new FileOutputStream(f);
			InputStream in = ftpClient.retrieveFileStream(new String(remote
					.getBytes(encode), encode));
			byte[] bytes = new byte[1024];
			int c;
			while ((c = in.read(bytes)) != -1) {
				out.write(bytes, 0, c);
			}
			in.close();
			out.close();
			boolean isDone = ftpClient.completePendingCommand();
			if (isDone) {
				ftpStatus = FTPStatus.DOWNLOAD_NEW_SUCCESS;
			} else {
				ftpStatus = FTPStatus.DOWNLOAD_NEW_FAILED;
			}
		}
		log.debug("ftp status: " + ftpStatus);
		return;
	}

	/**
	 * 下载
	 * @param host 
	 * @param port 
	 * @param username 
	 * @param password 
	 * @param remote 
	 * @param local 
	 * @throws IOException 
	 */
	public void downloadNoConn(String host, int port, String username,
			String password, String remote, String local) throws IOException {

		FTPClient ftp = new FTPClient();
		
		if (ConfigFactory.getBoolean("common.debug")) {
			ftp.addProtocolCommandListener(new PrintCommandListener(
					new PrintWriter(System.out)));
		}
		try {
			int reply;
			ftp.connect(host, port);
			// 如果采用默认端口，可以使用ftp.connect(url)的方式直接连接FTP服务器
			ftp.login(username, password);
			reply = ftp.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftp.disconnect();
				throw new IOException("connect ftp fail");
			}
			
			ftp.enterLocalPassiveMode();
			// 设置以二进制方式传输
			try {
				ftp.setFileType(FTP.BINARY_FILE_TYPE);
			} catch (IOException e1) {
				log.error("{}", e1);
				return;
			}

			FTPFile[] files = ftp.listFiles(new String(remote
					.getBytes(encode), encode));
			if (files.length != 1) {
				throw new IOException("not found ftpfile:" + remote);
			}

			File localFile = new File(local);

			OutputStream out = new FileOutputStream(localFile);
			InputStream in = ftp.retrieveFileStream(new String(remote
					.getBytes("UTF-8"), "UTF-8"));
			byte[] bytes = new byte[1024];
			int c;
			while ((c = in.read(bytes)) != -1) {
				out.write(bytes, 0, c);
			}
			in.close();
			out.close();
			ftp.logout();
		} catch (IOException e) {
			throw e;
		} finally {
			if (ftp.isConnected()) {
				try {
					ftp.disconnect();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}

	}


	@Override
	public void upload(String local, String remote) throws IOException {

		boolean isSupportBroke = false;
		// 设置主动模式
		ftpClient.enterLocalActiveMode();

		// 设置以二进制流的方式传输
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		// 设置编码
		ftpClient.setControlEncoding(encode);
		ftpClient.changeWorkingDirectory("/");
		int result;
		// 对远程目录的处理
		String remoteFileName = remote;
		if (remote.contains("/")) {
			remoteFileName = remote.substring(remote.lastIndexOf("/") + 1);
			// 创建服务器远程目录结构，创建失败直接返回
			if (createDirecroty(remote, ftpClient) == FTPStatus.CREATE_DIRECTORY_FAIL) {
				// return FTPStatus.Create_Directory_Fail;
				return;
			}
		}
		// 检查远程是否存在文件
		FTPFile[] files = ftpClient.listFiles(new String(remoteFileName
				.getBytes(encode), encode));
		if (isSupportBroke && files.length == 1) {
			long remoteSize = files[0].getSize();
			File f = new File(local);
			long localSize = f.length();
			if (remoteSize == localSize) {
				// return FTPStatus.File_Exits;
				return;
			}
			if (remoteSize > localSize) {
				// return FTPStatus.Remote_Bigger_Local;
				return;
			}
			// 尝试移动文件内读取指针,实现断点续传
			result = uploadFile(remoteFileName, f, ftpClient, remoteSize);
			// 如果断点续传没有成功，则删除服务器上文件，重新上传
			if (result == FTPStatus.UPLOAD_FROM_BREAK_FAILED) {
				if (!ftpClient.deleteFile(remoteFileName)) {
					// return FTPStatus.Delete_Remote_Faild;
					return;
				}
				result = uploadFile(remoteFileName, f, ftpClient, 0);
			}
		} else {
			if (files.length == 1) {
				result = FTPStatus.REMOTE_FILE_ALREADY_EXIST;
			} else {
				result = uploadFile(remoteFileName, new File(local), ftpClient,
						0);
			}
		}
		// return result;
		return;
	}

	@Override
	public void disconnect() throws IOException {
		if (ftpClient != null && ftpClient.isConnected()) {
			try {
				ftpClient.logout();
			} catch (Exception e) {
				log.error("{}", e);
			}
			log.info("close ftpclient");
			ftpClient.disconnect();
		}
	}

	@Override
	public ArrayList<String> listNames(String remotePath) throws IOException {

		ArrayList<String> files = new ArrayList<String>();
		// 设置主动传输
		ftpClient.enterLocalPassiveMode();
		// 设置以二进制流的方式传输
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		FTPFile[] tmpres = ftpClient.listFiles(remotePath);
		for (FTPFile f : tmpres) {
			files.add(f.getName());
		}
		return files;
	}

	/**
	 * @param path 
	 * @return
	 * @throws IOException 
	 */
	public FTPFile[] getFileList(String path) throws IOException {
		// 设置主动传输
		ftpClient.enterLocalActiveMode();
		// 设置以二进制流的方式传输
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		FTPFile[] ftpFiles = ftpClient.listFiles(path);

		return ftpFiles;
	}

	/**
	 * 递归创建ftp服务器目录
	 * 
	 * @param remote 
	 * @param ftpclient 
	 * @return 
	 * @throws IOException 
	 */
	public int createDirecroty(String remote, FTPClient ftpclient)
			throws IOException {
		int status = FTPStatus.CREATE_DIRECTORY_SUCCESS;
		String directory = remote.substring(0, remote.lastIndexOf("/") + 1);
		if (!directory.equalsIgnoreCase("/")
				&& !ftpclient.changeWorkingDirectory(new String(directory
						.getBytes(encode), encode))) {
			// 如果远程目录不存在，则递归创建远程服务器目录
			int start = 0;
			int end = 0;
			if (directory.startsWith("/")) {
				start = 1;
			} else {
				start = 0;
			}
			end = directory.indexOf("/", start);
			while (true) {
				String subDirectory = new String(remote.substring(start, end)
						.getBytes(encode), encode);

				if (!ftpclient.changeWorkingDirectory(subDirectory)) {
					if (ftpclient.makeDirectory(subDirectory)) {
						ftpclient.changeWorkingDirectory(subDirectory);
					} else {
						log.error("创建目录失败:" + subDirectory);
						return FTPStatus.CREATE_DIRECTORY_FAIL;
					}
				}
				start = end + 1;
				end = directory.indexOf("/", start);
				// 检查所有目录是否创建完毕
				if (end <= start) {
					break;
				}
			}
		}
		return status;
	}


	/**
	 * @param remoteFile	待上传到FTP服务器上的路径
	 * @param localFile		待上传文件所在的本地路径
	 * @param ftpclient		上传客户端
	 * @param remoteSize	大小
	 * @return	返回上传状态
	 * @throws IOException	异常
	 */
	@SuppressWarnings("resource")
	public int uploadFile(String remoteFile, File localFile,
			FTPClient ftpclient, long remoteSize) throws IOException {
		int status;
		RandomAccessFile raf = new RandomAccessFile(localFile, "r");
		OutputStream out = ftpclient.storeFileStream(new String(remoteFile
				.getBytes(encode), encode));
		if (out == null) {
			log.error("上传文件失败:" + remoteFile);
			return FTPStatus.UPLOAD_NEW_FILE_FAILED;
		}
		if (remoteSize > 0) {
			ftpclient.setRestartOffset(remoteSize);
			raf.seek(remoteSize);
		}
		byte[] bytes = new byte[1024];
		int c;
		while ((c = raf.read(bytes)) != -1) {
			out.write(bytes, 0, c);
		}
		out.flush();
		raf.close();
		out.close();
		boolean result = ftpclient.completePendingCommand();
		ftpclient.changeToParentDirectory();
		if (remoteSize > 0) {
			status = result ? FTPStatus.UPLOAD_FROM_BREAK_SUCCESS
					: FTPStatus.UPLOAD_FROM_BREAK_FAILED;
		} else {
			status = result ? FTPStatus.UPLOAD_NEW_FILE_SUCCESS
					: FTPStatus.UPLOAD_NEW_FILE_FAILED;
		}
		return status;
	}
}
