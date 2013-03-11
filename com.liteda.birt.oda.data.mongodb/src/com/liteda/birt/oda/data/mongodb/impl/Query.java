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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.MissingFormatArgumentException;

import org.eclipse.core.runtime.IStatus;
import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.SortSpec;
import org.eclipse.datatools.connectivity.oda.spec.QuerySpecification;

import com.liteda.birt.oda.data.mongodb.Activator;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Implementation class of IQuery for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded implementation that returns a pre-defined set of
 * meta-data and query results. A custom ODA driver is expected to implement own data source specific behavior in its
 * place.
 * 
 * @author dmitrijz
 */
public class Query implements IQuery {

	private int m_maxRows;
	private String m_preparedText;
	private String queryString;

	private DB db;
	private DBCursor cursor;
	private DBObject metadata;
	private DBCollection collection;

	private List<Object> params = new ArrayList<Object>(10);
	private List<Object> paramsTypes = new ArrayList<Object>(10);

	/**
	 * Default constructor.
	 * 
	 * @param db
	 */
	public Query(DB db) {
		this.db = db;
	}

	/** {@inheritDoc} */
	@Override
	public void prepare(String queryText) throws OdaException {
		try {
			m_preparedText = queryText;
			//
			String collectionName = "";
			if ((queryText.contains("(")) && (queryText.contains(")"))) {
				collectionName = queryText.substring(0, queryText.indexOf("(")).trim();
				queryString = queryText.substring(queryText.indexOf("(") + 1, queryText.lastIndexOf(")")).trim();
			} else {
				collectionName = queryText.trim();
				queryString = null;
			}

			//
			collection = db.getCollection(collectionName);
			if (collection == null) {
				throw new IllegalArgumentException("collection " + collectionName + " could not be found");
			}
			//
			metadata = collection.findOne();
			if (metadata == null) {
				throw new IllegalArgumentException("collection " + collectionName
						+ " is empty, unable to extract metadata");
			}
		} catch (Exception e) {
			Activator.logError(e, "unable to execute query %s", queryText);
			throw new OdaException(e);
		}
	}

	/** {@inheritDoc} */
	@Override
	public void setAppContext(Object context) throws OdaException {
		// do nothing; assumes no support for pass-through context
	}

	/** {@inheritDoc} */
	@Override
	public void close() throws OdaException {
		m_preparedText = null;
		metadata = null;
		queryString = null;
		cursor = null;
		collection = null;
	}

	/** {@inheritDoc} */
	@Override
	public IResultSetMetaData getMetaData() throws OdaException {
		return new ResultSetMetaData(metadata);
	}

	/** {@inheritDoc} */
	@Override
	public IResultSet executeQuery() throws OdaException {
		// {resTypeId: {$ne: 10125} }
		// String queryString = "{resTypeId: {$ne: %1$s} }";
		// queryString = String.format(queryString, params.toArray());

		try {
			if (queryString != null) {
				queryString = String.format(queryString, params.toArray());
				DBObject query = (DBObject) JSON.parse(queryString);
				cursor = collection.find(query);
			} else {
				cursor = collection.find();
			}

			if (cursor == null) {
				Activator.log(IStatus.WARNING, "No result set is return for query %s", null, queryString);
			} 
			
			IResultSet resultSet = new ResultSet(metadata, cursor);
			resultSet.setMaxRows(getMaxRows());
			return resultSet;
		} catch (MissingFormatArgumentException e) {
			// noop, since params are not set yet
			
			IResultSet resultSet = new ResultSet(metadata, cursor);
			resultSet.setMaxRows(getMaxRows());
			//
			return resultSet;
		} catch (Exception e) {
			e.printStackTrace();
			Activator.logError(e, "unable to execute query %s", queryString);
			throw new OdaException(e);
		}
	}

	/** {@inheritDoc} */
	@Override
	public void setProperty(String name, String value) throws OdaException {
		// do nothing; assumes no data set query property
	}

	/** {@inheritDoc} */
	@Override
	public void setMaxRows(int max) throws OdaException {
		m_maxRows = max;
	}

	/** {@inheritDoc} */
	@Override
	public int getMaxRows() throws OdaException {
		return m_maxRows;
	}

	/** {@inheritDoc} */
	@Override
	public void clearInParameters() throws OdaException {
		params.clear();
	}

	/** {@inheritDoc} */
	@Override
	public void setInt(String parameterName, int value) throws OdaException {
	}

	/** {@inheritDoc} */
	@Override
	public void setInt(int parameterId, int value) throws OdaException {
		addParam(parameterId, value, java.sql.Types.INTEGER);
	}

	/** {@inheritDoc} */
	@Override
	public void setDouble(String parameterName, double value) throws OdaException {
	}

	/** {@inheritDoc} */
	@Override
	public void setDouble(int parameterId, double value) throws OdaException {
		addParam(parameterId, value, java.sql.Types.DOUBLE);
	}

	/** {@inheritDoc} */
	@Override
	public void setBigDecimal(String parameterName, BigDecimal value) throws OdaException {
	}

	/** {@inheritDoc} */
	@Override
	public void setBigDecimal(int parameterId, BigDecimal value) throws OdaException {
		addParam(parameterId, value.toBigInteger().intValue(), java.sql.Types.BIGINT);
	}

	/** {@inheritDoc} */
	@Override
	public void setString(String parameterName, String value) throws OdaException {
	}

	/** {@inheritDoc} */
	@Override
	public void setString(int parameterId, String value) throws OdaException {
		addParam(parameterId, value, java.sql.Types.VARCHAR);
	}

	/** {@inheritDoc} */
	@Override
	public void setDate(String parameterName, Date value) throws OdaException {
	}

	/** {@inheritDoc} */
	@Override
	public void setDate(int parameterId, Date value) throws OdaException {
		// only applies to input parameter
	}

	/** {@inheritDoc} */
	@Override
	public void setTime(String parameterName, Time value) throws OdaException {
	}

	/** {@inheritDoc} */
	@Override
	public void setTime(int parameterId, Time value) throws OdaException {
		// only applies to input parameter
	}

	/** {@inheritDoc} */
	@Override
	public void setTimestamp(String parameterName, Timestamp value) throws OdaException {
	}

	/** {@inheritDoc} */
	@Override
	public void setTimestamp(int parameterId, Timestamp value) throws OdaException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'");
		addParam(parameterId, sdf.format(new java.util.Date(value.getTime())), java.sql.Types.TIMESTAMP);
	}

	/** {@inheritDoc} */
	@Override
	public void setBoolean(String parameterName, boolean value) throws OdaException {
	}

	/** {@inheritDoc} */
	@Override
	public void setBoolean(int parameterId, boolean value) throws OdaException {
		addParam(parameterId, Boolean.toString(value), java.sql.Types.VARCHAR);
	}

	/** {@inheritDoc} */
	@Override
	public void setObject(String parameterName, Object value) throws OdaException {
	}

	/** {@inheritDoc} */
	@Override
	public void setObject(int parameterId, Object value) throws OdaException {
	}

	/** {@inheritDoc} */
	@Override
	public void setNull(String parameterName) throws OdaException {
		// names are not supported
	}

	/** {@inheritDoc} */
	@Override
	public void setNull(int parameterId) throws OdaException {
		addParam(parameterId, null, java.sql.Types.INTEGER);
	}

	/** {@inheritDoc} */
	@Override
	public int findInParameter(String parameterName) throws OdaException {
		return 0;
	}

	/** {@inheritDoc} */
	@Override
	public IParameterMetaData getParameterMetaData() throws OdaException {
		int size = 0;
		if (queryString != null) {
			size = queryString.replace("%%", "").split("%").length - 1;
		}
		return new ParameterMetaData(params, paramsTypes, size);
	}

	/** {@inheritDoc} */
	@Override
	public void setSortSpec(SortSpec sortBy) throws OdaException {
		// only applies to sorting, assumes not supported
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public SortSpec getSortSpec() throws OdaException {
		// only applies to sorting
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void setSpecification(QuerySpecification querySpec) throws OdaException, UnsupportedOperationException {
		// assumes no support
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public QuerySpecification getSpecification() {
		// assumes no support
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public String getEffectiveQueryText() {
		return m_preparedText;
	}

	/** {@inheritDoc} */
	@Override
	public void cancel() throws OdaException, UnsupportedOperationException {
		// assumes unable to cancel while executing a query
		throw new UnsupportedOperationException();
	}

	protected void addParam(int indx, Object value, int type) {
		if ((params.size() < indx) || (params.get(indx - 1) == null)) {
			params.add(indx - 1, value);
			paramsTypes.add(indx - 1, type);
		} else {
			params.set(indx - 1, value);
			paramsTypes.set(indx - 1, type);
		}
	}

}
