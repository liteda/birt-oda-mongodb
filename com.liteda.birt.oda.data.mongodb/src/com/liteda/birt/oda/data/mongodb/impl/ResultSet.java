/*
 * Copyright (c) 2013 UAB Liteda (Ltd.)
 * All Rights Reserved. This source code is the confidential property of UAB Liteda.  All proprietary rights,
 * including but not limited to any trade secret, copyright, patent or trademark rights in and related to this
 * source code are the property of UAB Liteda.  This source code is not to be used, disclosed or reproduced in
 * any form without the express written consent of UAB Liteda.
 */

package com.liteda.birt.oda.data.mongodb.impl;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.eclipse.datatools.connectivity.oda.IBlob;
import org.eclipse.datatools.connectivity.oda.IClob;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Implementation class of IResultSet for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded implementation that returns a pre-defined set of
 * meta-data and query results. A custom ODA driver is expected to implement own data source specific behavior in its
 * place.
 * 
 * @author dmitrijz
 */
public class ResultSet implements IResultSet {

	private int m_maxRows;
	private int m_currentRowId;

	//
	private final IResultSetMetaData metadata;
	private final DBCursor cursor;

	//
	private DBObject row;

	/**
	 * Default constructor.
	 * 
	 * @param metadata
	 * @param cursor
	 */
	public ResultSet(DBObject metadata, DBCursor cursor) {
		this.metadata = new ResultSetMetaData(metadata);
		this.cursor = cursor;
	}

	/** {@inheritDoc} */
	@Override
	public IResultSetMetaData getMetaData() throws OdaException {
		return metadata;
	}

	/** {@inheritDoc} */
	@Override
	public void setMaxRows(int max) throws OdaException {
		m_maxRows = max;
	}

	/**
	 * Returns the maximum number of rows that can be fetched from this result set.
	 * 
	 * @return the maximum number of rows to fetch.
	 */
	protected int getMaxRows() {
		return m_maxRows;
	}

	/** {@inheritDoc} */
	@Override
	public boolean next() throws OdaException {
		int maxRows = getMaxRows();
		if ((maxRows > 0) && (m_currentRowId > maxRows)) {
			return false;
		}
		
		if (cursor == null){
			return false;
		}

		if (cursor.hasNext()) {
			row = this.cursor.next();
			m_currentRowId += 1;
			return true;
		}

		return false;
	}

	/** {@inheritDoc} */
	@Override
	public void close() throws OdaException {
		m_currentRowId = 0; // reset row counter
	}

	/** {@inheritDoc} */
	@Override
	public int getRow() throws OdaException {
		return m_currentRowId;
	}

	/** {@inheritDoc} */
	@Override
	public String getString(int index) throws OdaException {
		return getString(metadata.getColumnName(index));
	}

	/** {@inheritDoc} */
	@Override
	public String getString(String columnName) throws OdaException {
		return (row.get(columnName) != null) ? (String) row.get(columnName).toString() : "";
	}

	/** {@inheritDoc} */
	@Override
	public int getInt(int index) throws OdaException {
		return getInt(metadata.getColumnName(index));
	}

	/** {@inheritDoc} */
	@Override
	public int getInt(String columnName) throws OdaException {
		return (Integer) row.get(columnName);
	}

	/** {@inheritDoc} */
	@Override
	public double getDouble(int index) throws OdaException {
		return getDouble(metadata.getColumnName(index));
	}

	/** {@inheritDoc} */
	@Override
	public double getDouble(String columnName) throws OdaException {
		return (Double) row.get(columnName);
	}

	/** {@inheritDoc} */
	@Override
	public BigDecimal getBigDecimal(int index) throws OdaException {
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public BigDecimal getBigDecimal(String columnName) throws OdaException {
		return getBigDecimal(findColumn(columnName));
	}

	/** {@inheritDoc} */
	@Override
	public Date getDate(int index) throws OdaException {
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public Date getDate(String columnName) throws OdaException {
		return getDate(findColumn(columnName));
	}

	/** {@inheritDoc} */
	@Override
	public Time getTime(int index) throws OdaException {
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public Time getTime(String columnName) throws OdaException {
		return getTime(findColumn(columnName));
	}

	/** {@inheritDoc} */
	@Override
	public Timestamp getTimestamp(int index) throws OdaException {
		return getTimestamp(metadata.getColumnName(index));
	}

	/** {@inheritDoc} */
	@Override
	public Timestamp getTimestamp(String columnName) throws OdaException {
		Object value = row.get(columnName);
		if (value instanceof java.util.Date) {
			return new Timestamp(((java.util.Date) value).getTime());
		} else if (value instanceof Date) {
			return new Timestamp(((Date) value).getTime());
		} else {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z");
			try {
				return new Timestamp((sdf.parse((String) value)).getTime());
			} catch (ParseException e) {
				throw new OdaException("Unable to transform value " + value + " to timestamp, invalid format");
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public IBlob getBlob(int index) throws OdaException {
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public IBlob getBlob(String columnName) throws OdaException {
		return getBlob(findColumn(columnName));
	}

	/** {@inheritDoc} */
	@Override
	public IClob getClob(int index) throws OdaException {
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public IClob getClob(String columnName) throws OdaException {
		return getClob(findColumn(columnName));
	}

	/** {@inheritDoc} */
	@Override
	public boolean getBoolean(int index) throws OdaException {
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public boolean getBoolean(String columnName) throws OdaException {
		return getBoolean(findColumn(columnName));
	}

	/** {@inheritDoc} */
	@Override
	public Object getObject(int index) throws OdaException {
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public Object getObject(String columnName) throws OdaException {
		return getObject(findColumn(columnName));
	}

	/** {@inheritDoc} */
	@Override
	public boolean wasNull() throws OdaException {
		// hard-coded for demo purpose
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public int findColumn(String arg) throws OdaException {
		return 0;
	}

}
