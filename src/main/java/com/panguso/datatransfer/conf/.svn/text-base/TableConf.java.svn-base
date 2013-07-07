/**
 *  Copyright (c)  2011-2020 Panguso, Inc.
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of Panguso, 
 *  Inc. ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the license agreement you entered into with Panguso.
 */
package com.panguso.datatransfer.conf;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.panguso.datatransfer.config.Common;
import com.panguso.datatransfer.config.ConfigFactory;
import com.panguso.datatransfer.util.CommonUtil;

/**
 * @author zhaopeng
 *
 */
public class TableConf {
	private static final Logger LOG = LoggerFactory.getLogger(TableConf.class);	
	private String name = null;
	private String ds = null;
	private String primaryKey = null;
	private FieldConf fieldConf = null;
	private String prefix = null;
	private Map<String, Integer> fieldLenMap = null;
		
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDs() {
		return ds;
	}

	public void setDs(String ds) {
		this.ds = ds;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}
	
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public FieldConf getFieldConf() {
		return fieldConf;
	}

	public void setFieldConf(FieldConf fieldConf) {
		this.fieldConf = fieldConf;
	}

	public Map<String, Integer> getFieldLenMap() {
		return fieldLenMap;
	}

	public void setFieldLenMap(Map<String, Integer> fieldLenMap) {
		this.fieldLenMap = fieldLenMap;
	}


	/**
	 * @function 构造函数
	 * @param prefix 前缀
	 */
	public TableConf(String prefix) {
		this.prefix = prefix;
		fieldLenMap = new HashMap<String, Integer>();
	}
	
	
	/**
	 * @function 初始化
	 */
	public void init() {
		if (null == prefix) {
			LOG.error("init TableConf error, prefix is null!");
			System.exit(-1);
		}
				
		//初始化表名称
		String sName = ConfigFactory.getString(prefix + Common.NAME);
		if (!CommonUtil.isNullOrEmpty(sName)) {
			this.name = sName.trim().toLowerCase();
		} else {
			LOG.error("init TableConf error, config value for naem tag is null or empty!");
			System.exit(-1);
		}
		LOG.debug("Begin to init TableConf of {}... ", name);
		
		//初始化表所属数据库信息
		String sDS = ConfigFactory.getString(prefix + Common.DB);
		if (!CommonUtil.isNullOrEmpty(sDS)) {
			this.ds = sDS.trim().toLowerCase();
		} else {
			LOG.error("init TableConf of {} error, config value for ds tag is null or empty!", name);
			System.exit(-1);
		}
		
		//初始化主键
		String primarykey  = ConfigFactory.getString(prefix + Common.PRIMARYKEY);
		if (!CommonUtil.isNullOrEmpty(sDS)) {
			this.primaryKey = primarykey.trim().toLowerCase();
		} else {
			LOG.error("init TableConf of {} error, config value for primay_key tag is null or empty!", name);
			System.exit(-1);
		}
		
		//初始化字段长度信息
		String fieldLenTag = prefix + Common.FIELDLEN;
		Map<String, String> map = CommonUtil.getMapFromTag(fieldLenTag);
		
		if (CommonUtil.isNullOrEmpty(map)) {
			LOG.warn("init TableConf of {} warning, config value for field_length tag is empty!", name);
		} else {
			if (null == fieldLenMap) {
				fieldLenMap = new HashMap<String, Integer>();
			}
			//转换Map<String, String> to Map<String, Integer>
			for (String key : map.keySet()) {
				String value = map.get(key);
				if (!CommonUtil.isNullOrEmpty(value) && value.matches(Common.REGEXINTEGER)) {
					fieldLenMap.put(key, Integer.valueOf(value));
				}
			}
		}
		
		//初始化表字段信息
		FieldConf fieldconf = new FieldConf(prefix);
		fieldconf.init();
		this.fieldConf = fieldconf;
		
		LOG.info("init TableConf of {} success", name);
	}
}
