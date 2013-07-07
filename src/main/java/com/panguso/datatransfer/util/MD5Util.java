/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liubing
 *
 */
public final class MD5Util {
	private static final char[] md5Chars = { '0', '1', '2', '3', '4', '5', '6', '7',
		'8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
	private static final Logger LOG = LoggerFactory.getLogger(MD5Util.class);
	private static MessageDigest messagedigest;
	
	/**
	 * 构造函数
	 */
	private MD5Util() {
		
	}
	
	/**
	 * @function 获取文件的MD5值
	 * @param file 要执行MD5的文件
	 * @return MD5值
	 */
	public static String getFileMD5String(File file) {
		try {
			messagedigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		String md5 = "";
		FileInputStream in = null;
		try {
			in = new FileInputStream(file);
			FileChannel ch = in.getChannel();
			MappedByteBuffer byteBuffer = ch.map(FileChannel.MapMode.READ_ONLY, 0,
					file.length());
			messagedigest.update(byteBuffer);
			md5 = bufferToHex(messagedigest.digest());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return md5;
	}
	
	/**
	 * @function 获取一个字符串的MD5值
	 * @param str 带计算MD5值的字符串
	 * @return 计算得到的MD5值
	 * @throws Exception
	 */
	public static String getStringMD5String(String str) {
		try {
			messagedigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		messagedigest.update(str.getBytes());
		return bufferToHex(messagedigest.digest());
	}
	
	/**
	 * @function 验证一个字符串和一个MD5码是否相等
	 * @param str 待检查的字符串
	 * @param md5 待比较的MD5值
	 * @return
	 * @throws Exception
	 */
	public static boolean check(String str, String md5) {
		return getStringMD5String(str).equals(md5);
	}
	
	/**
	 * @function 验证一个文件和一个MD5码是否相等
	 * @param file 待验证的文件
	 * @param md5 待验证的MD5值
	 * @return
	 */
	public static boolean check(File file, String md5) {
		String md5Value = getFileMD5String(file);
		LOG.debug("md5 of file: " + md5Value);
		return md5Value.equals(md5);
	}
	
	private static String bufferToHex(byte[] bytes) {
		return bufferToHex(bytes, 0, bytes.length);
	}
	
	private static String bufferToHex(byte[] bytes, int m, int n) {
		StringBuffer stringbuffer = new StringBuffer(2 * n);
		int k = m + n;
		for (int l = m; l < k; l++) {
			appendHexPair(bytes[l], stringbuffer);
		}
		return stringbuffer.toString();
	}
	
	private static void appendHexPair(byte bt, StringBuffer stringbuffer) {
		char c0 = md5Chars[(bt & 0xf0) >> 4];
		char c1 = md5Chars[bt & 0xf];
		stringbuffer.append(c0);
		stringbuffer.append(c1);
	}
	
	/*
	public static void main(String[] argv) throws Exception { 
		String path = "D:/Users/liubing/Data/20130409/full/20130409_mm_0.txt";
		String md5Path = "D:/Users/liubing/Data/20130409/full/20130409_mm_0.md5";
		
		List<String> lines = IOUtil.readFile(md5Path);
		if (CommonUtil.isNullOrEmpty(lines)) {
			System.out.println("Content of " + md5Path + " empty");
			return;
		}
		
//		MD5Util.getFileMD5String(new File(path));
		System.out.println(lines.get(0));
		System.out.println("md5: " + MD5Util.getFileMD5String(new File(path)));
	}
	*/
	
}