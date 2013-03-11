/*
 * Copyright (c) 2013 UAB Liteda (Ltd.)
 * All Rights Reserved. This source code is the confidential property of UAB Liteda.  All proprietary rights,
 * including but not limited to any trade secret, copyright, patent or trademark rights in and related to this
 * source code are the property of UAB Liteda.  This source code is not to be used, disclosed or reproduced in
 * any form without the express written consent of UAB Liteda.
 */

package com.liteda.birt.oda.data.mongodb.impl;

import org.eclipse.datatools.connectivity.oda.IConnection;
import org.eclipse.datatools.connectivity.oda.IDataSetMetaData;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.OdaException;

/**
 * Implementation class of IDataSetMetaData for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded implementation that assume this custom ODA data set
 * is capable of handling a query that returns a single result set and accepts scalar input parameters by index. A
 * custom ODA driver is expected to implement own data set specific behavior in its place.
 * 
 * @author dmitrijz
 */
public class DataSetMetaData implements IDataSetMetaData {

	private IConnection m_connection;

	/**
	 * Default constructor.
	 * 
	 * @param connection
	 */
	DataSetMetaData(IConnection connection) {
		m_connection = connection;
	}

	/** {@inheritDoc} */
	@Override
	public IConnection getConnection() throws OdaException {
		return m_connection;
	}

	/** {@inheritDoc} */
	@Override
	public IResultSet getDataSourceObjects(String catalog, String schema, String object, String version)
			throws OdaException {
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public int getDataSourceMajorVersion() throws OdaException {
		return 1;
	}

	/** {@inheritDoc} */
	@Override
	public int getDataSourceMinorVersion() throws OdaException {
		return 0;
	}

	/** {@inheritDoc} */
	@Override
	public String getDataSourceProductName() throws OdaException {
		return "MongoDB Data Source";
	}

	/** {@inheritDoc} */
	@Override
	public String getDataSourceProductVersion() throws OdaException {
		return Integer.toString(getDataSourceMajorVersion()) + "." + //$NON-NLS-1$
				Integer.toString(getDataSourceMinorVersion());
	}

	/** {@inheritDoc} */
	@Override
	public int getSQLStateType() throws OdaException {
		return IDataSetMetaData.sqlStateSQL99;
	}

	/** {@inheritDoc} */
	@Override
	public boolean supportsMultipleResultSets() throws OdaException {
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public boolean supportsMultipleOpenResults() throws OdaException {
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public boolean supportsNamedResultSets() throws OdaException {
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public boolean supportsNamedParameters() throws OdaException {
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public boolean supportsInParameters() throws OdaException {
		return true;
	}

	/** {@inheritDoc} */
	@Override
	public boolean supportsOutParameters() throws OdaException {
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public int getSortMode() {
		return IDataSetMetaData.sortModeNone;
	}

}
