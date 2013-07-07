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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.panguso.datatransfer.config.ConfigFactory;


/**
 * @author liubing
 *
 */
public class SFTPClient extends BaseFTPClient {

	private Logger log = LoggerFactory.getLogger(SFTPClient.class);
	private ChannelSftp sftp = null;
	private Session sshSession = null;

	/**
	 * connect to server via sftp
	 * 
	 * @param hostname 
	 * @param port 
	 * @param username 
	 * @param password 
	 * @return 
	 * @param encode 
	 * @throws Exception 
	 */
	public boolean connect(String hostname, int port, String username,
			String password, String encode) throws Exception {
		JSch jsch = new JSch();
		sshSession = jsch.getSession(username, hostname, port);
		// 设置密码
		sshSession.setPassword(password);
		Properties sshConfig = new Properties();
		sshConfig.put("StrictHostKeyChecking", "no");
		// 设置properties
		sshSession.setConfig(sshConfig);
		// 设置超时
		sshSession.setTimeout(ConfigFactory.getInt("common.conectiontimeout",
				60000));
		log.debug("to connect: host:" + hostname + " port:" + port + " username:" + username);
		sshSession.connect();
		Channel channel = sshSession.openChannel("sftp");
		channel.connect();
		sftp = (ChannelSftp) channel;
		return true;
	}

	/**
	 * Disconnect with server
	 */
	public void disconnect() {
		if (this.sftp != null) {
			this.sftp.disconnect();
			this.sftp = null;
		}
		if (this.sshSession != null) {
			this.sshSession.disconnect();
			this.sshSession = null;
		}
	}

	/**
	 * download remote file to local
	 * 
	 * @param remote 
	 * @param local 
	 * @throws Exception 
	 */
	public void download(String remote, String local) throws Exception {
		// 先删除已经存在的本地文件
		File f = new File(local);
		if (f.exists()) {
			f.delete();
		}
		File file = new File(local);
		sftp.get(remote, new FileOutputStream(file));
	}

	/**
	 * upload local file to server
	 * 
	 * @param local 
	 * @param remote 
	 * @throws Exception 
	 */
	public void upload(String local, String remote) throws Exception {
		File file = new File(local);
		if (file.isFile()) {
			File rfile = new File(remote);
			String rpath = rfile.getParent();
			createDirecroty(rpath, sftp);
			this.sftp.put(new FileInputStream(file), remote);
		}
	}

	/**
	 * create Directory
	 * 
	 * @param filepath 
	 * @param sFtp 
	 */
	private void createDirecroty(String remote, ChannelSftp sFtp)
			throws Exception {
		boolean bcreated = false;
		boolean bparent = false;
		File file = new File(remote);
		String ppath = file.getParent();
		try {
			this.sftp.cd(ppath);
			bparent = true;
		} catch (SftpException e1) {
			bparent = false;
		}
		try {
			if (bparent) {
				try {
					this.sftp.cd(remote);
					bcreated = true;
				} catch (Exception e) {
					bcreated = false;
				}
				if (!bcreated) {
					this.sftp.mkdir(remote);
					bcreated = true;
					log.info("创建目录成功:" + remote);
				}
				return;
			} else {
				createDirecroty(ppath, sFtp);
				this.sftp.cd(ppath);
				this.sftp.mkdir(remote);
			}
		} catch (SftpException e) {
			log.info("创建目录失败:" + remote);
			throw e;
		}

		try {
			this.sftp.cd(remote);
		} catch (SftpException e) {
			log.info("创建目录失败:" + remote);
			throw e;
		}
	}

	/**
	 * get all the files need to be upload or download
	 * 
	 * @param file
	 * @return
	 * @param remotePath 
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<String> listNames(String remotePath) {
		ArrayList<String> files = new ArrayList<String>();
		
		Vector<LsEntry> fileList = null;
		try {
			fileList = sftp.ls(remotePath);
		} catch (SftpException e) {
			log.error("ftp list files error!");
			e.printStackTrace();
			return files;
		}
		if (null != fileList) {
			List<LsEntry> tmpres = fileList.subList(0, fileList.size());
			for (LsEntry ls : tmpres) {
				files.add(ls.getFilename());
			}
		}
		return files;
	}
}
