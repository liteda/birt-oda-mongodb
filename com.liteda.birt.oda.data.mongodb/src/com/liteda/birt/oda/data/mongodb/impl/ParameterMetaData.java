/*
 * Copyright (c) 2013 UAB Liteda (Ltd.)
 * All Rights Reserved. This source code is the confidential property of UAB Liteda.  All proprietary rights,
 * including but not limited to any trade secret, copyright, patent or trademark rights in and related to this
 * source code are the property of UAB Liteda.  This source code is not to be used, disclosed or reproduced in
 * any form without the express written consent of UAB Liteda.
 */

package com.liteda.birt.oda.data.mongodb.impl;

import java.util.List;

import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;

/**
 * Implementation class of IParameterMetaData for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded implementation that returns a pre-defined set of
 * meta-data and query results. A custom ODA driver is expected to implement own data source specific behavior in its
 * place.
 * 
 * @author dmitrijz
 */
public class ParameterMetaData implements IParameterMetaData {

	// private List<Object> params;
	// private List<Object> paramsTypes;
	private int size;

	/**
	 * Default constructor.
	 * 
	 * @param params
	 * @param paramsTypes
	 */
	public ParameterMetaData(List<Object> params, List<Object> paramsTypes, int size) {
		// this.params = params;
		// this.paramsTypes = paramsTypes;
		this.size = size;
	}

	/** {@inheritDoc} */
	@Override
	public int getParameterCount() throws OdaException {
		return size;
	}

	/** {@inheritDoc} */
	@Override
	public int getParameterMode(int param) throws OdaException {
		return IParameterMetaData.parameterModeIn;
	}

	/** {@inheritDoc} */
	@Override
	public String getParameterName(int param) throws OdaException {
		return null; // name is not available
	}

	/** {@inheritDoc} */
	@Override
	public int getParameterType(int param) throws OdaException {
		return java.sql.Types.CHAR; // as defined in data set extension manifest
		// return (Integer) paramsTypes.get(param - 1);
	}

	/** {@inheritDoc} */
	@Override
	public String getParameterTypeName(int param) throws OdaException {
		int nativeTypeCode = getParameterType(param);
		return Driver.getNativeDataTypeName(nativeTypeCode);
	}

	/** {@inheritDoc} */
	@Override
	public int getPrecision(int param) throws OdaException {
		return -1;
	}

	/** {@inheritDoc} */
	@Override
	public int getScale(int param) throws OdaException {
		return -1;
	}

	/** {@inheritDoc} */
	@Override
	public int isNullable(int param) throws OdaException {
		return IParameterMetaData.parameterNullableUnknown;
	}

}
