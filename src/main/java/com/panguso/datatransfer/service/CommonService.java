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

/**
 * @author zhaopeng
 *
 */
public interface CommonService {

	/**
	 * @return 该服务是否完成
	 */
	boolean doService();
	
	/**
	 * @param prefix XPath前缀
	 */
	void setPrefix(String prefix);
}
