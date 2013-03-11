/*
 * Copyright (c) 2013 UAB Liteda (Ltd.)
 * All Rights Reserved. This source code is the confidential property of UAB Liteda.  All proprietary rights,
 * including but not limited to any trade secret, copyright, patent or trademark rights in and related to this
 * source code are the property of UAB Liteda.  This source code is not to be used, disclosed or reproduced in
 * any form without the express written consent of UAB Liteda.
 */
package com.liteda.birt.oda.data.mongodb.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;

import com.mongodb.DBObject;

/**
 * Implementation class of IResultSetMetaData for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded implementation that returns a pre-defined set of
 * meta-data and query results. A custom ODA driver is expected to implement own data source specific behavior in its
 * place.
 * 
 * @author dmitrijz
 */
public class ResultSetMetaData implements IResultSetMetaData {

	private List<String> names = new ArrayList<String>();
	private List<Integer> types = new ArrayList<Integer>();

	public ResultSetMetaData(DBObject metadata) {
		//
		for (String keyField : metadata.keySet()) {
			names.add(keyField);
			Integer dataType = getDataType(metadata.get(keyField));
			types.add(dataType);
		}

	}

	private Integer getDataType(Object value) {
		if ((value instanceof Integer)) {
			return Integer.valueOf(4);
		}
		if ((value instanceof Double)) {
			return Integer.valueOf(8);
		}
		if ((value instanceof Float)) {
			return Integer.valueOf(3);
		}
		if ((value instanceof Date)) {
			return Integer.valueOf(93);
		}
		if ((value instanceof Boolean)) {
			return Integer.valueOf(16);
		}
		return Integer.valueOf(1); // string
	}

	/** {@inheritDoc} */
	@Override
	public int getColumnCount() throws OdaException {
		return names.size();
	}

	/** {@inheritDoc} */
	@Override
	public String getColumnName(int index) throws OdaException {
		return names.get(index - 1);
	}

	/** {@inheritDoc} */
	@Override
	public String getColumnLabel(int index) throws OdaException {
		return getColumnName(index);
	}

	/** {@inheritDoc} */
	@Override
	public int getColumnType(int index) throws OdaException {
		return types.get(index - 1);

	}

	/** {@inheritDoc} */
	@Override
	public String getColumnTypeName(int index) throws OdaException {
		int nativeTypeCode = getColumnType(index);
		return Driver.getNativeDataTypeName(nativeTypeCode);
	}

	/** {@inheritDoc} */
	@Override
	public int getColumnDisplayLength(int index) throws OdaException {
		return 8;
	}

	/** {@inheritDoc} */
	@Override
	public int getPrecision(int index) throws OdaException {
		return -1;
	}

	/** {@inheritDoc} */
	@Override
	public int getScale(int index) throws OdaException {
		return -1;
	}

	/** {@inheritDoc} */
	@Override
	public int isNullable(int index) throws OdaException {
		return IResultSetMetaData.columnNullableUnknown;
	}

}
