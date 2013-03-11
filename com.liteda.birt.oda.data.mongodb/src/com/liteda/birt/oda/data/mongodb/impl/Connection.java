/*
 * Copyright (c) 2013 UAB Liteda (Ltd.)
 * All Rights Reserved. This source code is the confidential property of UAB Liteda.  All proprietary rights,
 * including but not limited to any trade secret, copyright, patent or trademark rights in and related to this
 * source code are the property of UAB Liteda.  This source code is not to be used, disclosed or reproduced in
 * any form without the express written consent of UAB Liteda.
 */

package com.liteda.birt.oda.data.mongodb.impl;

import java.util.Properties;

import org.eclipse.datatools.connectivity.oda.IConnection;
import org.eclipse.datatools.connectivity.oda.IDataSetMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.OdaException;

import com.ibm.icu.util.ULocale;
import com.liteda.birt.oda.data.mongodb.Activator;
import com.mongodb.DB;
import com.mongodb.MongoClient;

/**
 * Implementation class of IConnection for an ODA runtime driver.
 * 
 * @author dmitrijz
 */
public class Connection implements IConnection {

	private boolean m_isOpen = false;

	/**
	 * holds the database connection
	 */
	private DB db;

	/** {@inheritDoc} */
	@Override
	public void open(Properties connProperties) throws OdaException {
		String host = connProperties.getProperty("host", "localhost");
		String port = connProperties.getProperty("port", "27017");
		String dbName = connProperties.getProperty("db", "test");
		String username = connProperties.getProperty("username");
		String password = connProperties.getProperty("password");
		//
		int portNo = 0;
		try {
			portNo = Integer.parseInt(port);
		} catch (NumberFormatException localNumberFormatException) {
			throw new OdaException("Invalid port number");
		}

		try {
			MongoClient client = new MongoClient(host, portNo);
			db = client.getDB(dbName);
			if ((username != null) && (!username.isEmpty())) {
				db.authenticate(username, password.toCharArray());
			}
		} catch (Exception e) {
			Activator.logError(e, "unable to connect to mongo instance %s:%s %s", host, port, dbName);
			throw new OdaException(String.format("unable to connect to mongo instance %s:%s %s", host, port, dbName));
		}

		m_isOpen = true;
	}

	/** {@inheritDoc} */
	@Override
	public void setAppContext(Object context) throws OdaException {
		// do nothing; assumes no support for pass-through context
	}

	/** {@inheritDoc} */
	@Override
	public void close() throws OdaException {
		if (db != null) {
			db.getMongo().close();
			db = null;
		}
		m_isOpen = false;
	}

	/** {@inheritDoc} */
	@Override
	public boolean isOpen() throws OdaException {
		return m_isOpen;
	}

	/** {@inheritDoc} */
	@Override
	public IDataSetMetaData getMetaData(String dataSetType) throws OdaException {
		return new DataSetMetaData(this);
	}

	/** {@inheritDoc} */
	@Override
	public IQuery newQuery(String dataSetType) throws OdaException {
		return new Query(this.db);
	}

	/** {@inheritDoc} */
	@Override
	public int getMaxQueries() throws OdaException {
		return 0; // no limit
	}

	/** {@inheritDoc} */
	@Override
	public void commit() throws OdaException {
		// do nothing; assumes no transaction support needed
	}

	/** {@inheritDoc} */
	@Override
	public void rollback() throws OdaException {
		// do nothing; assumes no transaction support needed
	}

	/** {@inheritDoc} */
	@Override
	public void setLocale(ULocale locale) throws OdaException {
		// do nothing; assumes no locale support
	}

}
