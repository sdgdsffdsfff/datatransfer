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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.conf.DownloadFileInfo;

/**
 * @author liubing
 *
 */
public final class IOUtil {
	private static final Logger LOG = LoggerFactory.getLogger(IOUtil.class);

	
	private IOUtil() {
		
	}
	
	/**
	 * @function 读取文件
	 * @param filePath 文件路径
	 * @param encoding 编码
	 * @return
	 * @throws IOException
	 */
	public static List<String> readFile(String filePath, String encoding) {
		File file = new File(filePath);
		List<String> lines = new ArrayList<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(file), encoding));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (line.trim().length() > 0) {
					lines.add(line.trim());
				}
			}
		} catch (IOException e) {
			LOG.error("IOUtil.readFile:", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					LOG.error("IOUtil.readFile:", e);
				}
			}
		}
		return lines;
	}
	

	/**
	 * @function 读取路径对应文件中的所有数据
	 * @param filePath 带读取文件的路径
	 * @return 若下载失败返回null, 若本次没有数据要下载则返回空（适用于增量下载）
	 */
	public static List<String> readFile(String filePath) {
		List<String> fileList = new ArrayList<String>();	
		//判断文件名是否正确
		if (CommonUtil.isNullOrEmpty(filePath)) {
			LOG.error("The dir {} is null or empty!", filePath);
			System.exit(-1);
		}
		//判断文件是否存在
		File file = new File(filePath);
		if (!file.exists()) {
			LOG.error("The dir {} is not exist!", filePath);
			return fileList;
		}
		//开始读取文件内容
		FileReader fr = null;
		BufferedReader br = null;
		String line = null;

		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			while ((line = br.readLine()) != null) {
				//LOG.debug("get file:" + line);
				fileList.add(line);
			}
		} catch (Exception e) {
			LOG.error("{}", e);
		} finally {
			try {
				br.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
			try {
				fr.close();
			} catch (Exception e3) {
				e3.printStackTrace();
			}
		}
		return fileList;
	}
	
	
	
	/**
	 * 读取监控文件内容
	 * @param fileName 带读取的文件名
	 * @param charset 字符集
	 * @param offset 偏移量
	 * @return
	 */
	public static String[] readLastLine(String fileName, String charset,
			int offset) {
		File file = new File(fileName);
		if (!file.exists() || file.isDirectory() || !file.canRead()) {
			return null;
		}

		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(file, "r");
			long len = raf.length();
			if (len == 0L) {
				return null;
			} else {
				String[] result = new String[offset];
				long startpos = len - 1;
				long endPos = len - 2;
				int index = offset - 1;
				while (startpos > 0 && index > -1) {
					startpos--;
					raf.seek(startpos);
					if (raf.readByte() == '\n') {
						byte[] bytes = new byte[(int) (endPos - startpos)];
						raf.read(bytes);
						String content = null;
						if (charset == null) {
							content = new String(bytes);
						} else {
							content = new String(bytes, charset);
						}
						result[index] = content;
						index--;
						endPos = startpos - 1;
					}
				}
				return result;
			}
		} catch (IOException e) {
			LOG.error("IOUtil.readLastLine:", e);
		} finally {
			if (raf != null) {
				try {
					raf.close();
				} catch (IOException e) {
					LOG.error("IOUtil.readLastLine:", e);
				}
			}
		}
		return null;
	}
	
	//
	/**
	 * @function 将错误信息写入core文件中
	 * @param errSqlList 错误信息列表
	 */
	public static void writeToCoreFile(List<String> errSqlList) {
		//获取系统路径
		String filePath = System.getProperty("datatransfer");
		if (CommonUtil.isNullOrEmpty(filePath)) {
			String rootPath = CommonUtil.getDefaultConfPath();
			filePath = rootPath;
		}
		//检查存储错误信息的errrecord文件夹是否存在
		String today = DateUtil.getToday();
		String parentPath = filePath + (File.separator + "errrecord" + File.separator + today); 
		File parentFile = new File(parentPath);
		if (!parentFile.exists()) {
			parentFile.mkdirs();
		}
		
		//判断是否存在以现在时间为名称的文件存在
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		String nowHour = sdf.format(new Date());
		filePath = parentPath + File.separator + nowHour + ".txt";
		LOG.debug("write error sql into file " + filePath);
		File file = new File(filePath);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("create file " + file.getAbsolutePath() + " error!");
			}
		}
		//将错误信息列表写入文件中
		BufferedWriter br = null;
		try {
			br = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(file, true), "UTF-8"));
			for (int i = 0; i < errSqlList.size(); i++) {
			//	LOG.debug("error_sql:{}", errSqlList.get(i));
				br.write(DateUtil.getNow() + ":" + ((String) errSqlList.get(i)).trim());
				br.newLine();
			}
			br.flush();
		} catch (IOException e) {
			LOG.error("IOUtil.writeToFile,", e);
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				LOG.error("IOUtil.writeToFile,", e);
			}
		}
	}
	

	/**
	 * @function 将下载列表downloadFileList的本地路径写入path路径指定的文件中
	 * @param filePath 待写入路径
	 * @param list 下载信息列表
	 */
	public static void writeToFile(String filePath, List<DownloadFileInfo> list) {
		if (CommonUtil.isNullOrEmpty(filePath) || null == list) {
			LOG.error("file path is null or list is null");
			return;
		}

		File file = new File(filePath);
		FileWriter fWriter = null;
		try {	
			fWriter = new FileWriter(file, true);
			for (DownloadFileInfo downloadInfo : list) {
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
	}
	
	/**
	 * @function 打印数组
	 * @param array 待打印的数组
	 */
	public static void printArray(Object[] array) {
		if (array != null) {
			StringBuilder sb = new StringBuilder();
			for (Object obj : array) {
				sb.append(obj.toString()).append("\t");
			}
			LOG.error(sb.toString());
		}
	}
	
	/*
	public static void main(String[] args){
		String filePath = "f:/TEST.TXT";
		
		List<DownloadFileInfo> list = new ArrayList<DownloadFileInfo>();
		IOUtil.writeToFile(filePath, list);
	}
	*/

}