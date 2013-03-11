/*
 * Copyright (c) 2013 UAB Liteda (Ltd.)
 * All Rights Reserved. This source code is the confidential property of UAB Liteda.  All proprietary rights,
 * including but not limited to any trade secret, copyright, patent or trademark rights in and related to this
 * source code are the property of UAB Liteda.  This source code is not to be used, disclosed or reproduced in
 * any form without the express written consent of UAB Liteda.
 */

package com.liteda.birt.oda.data.mongodb.impl;

import org.eclipse.datatools.connectivity.oda.IConnection;
import org.eclipse.datatools.connectivity.oda.IDriver;
import org.eclipse.datatools.connectivity.oda.LogConfiguration;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.util.manifest.DataTypeMapping;
import org.eclipse.datatools.connectivity.oda.util.manifest.ExtensionManifest;
import org.eclipse.datatools.connectivity.oda.util.manifest.ManifestExplorer;

/**
 * Implementation class of IDriver for an ODA runtime driver.
 * 
 * @author dmitrijz
 */
public class Driver implements IDriver {

	static String ODA_DATA_SOURCE_ID = "com.liteda.birt.oda.data.mongodb"; //$NON-NLS-1$

	/** {@inheritDoc} */
	@Override
	public IConnection getConnection(String dataSourceType) throws OdaException {
		// assumes that this driver supports only one type of data source,
		// ignores the specified dataSourceType
		return new Connection();
	}

	/** {@inheritDoc} */
	@Override
	public void setLogConfiguration(LogConfiguration logConfig) throws OdaException {
		// do nothing; assumes simple driver has no logging
	}

	/** {@inheritDoc} */
	@Override
	public int getMaxConnections() throws OdaException {
		return 0; // no limit
	}

	/** {@inheritDoc} */
	@Override
	public void setAppContext(Object context) throws OdaException {
		// do nothing; assumes no support for pass-through context
	}

	/**
	 * Returns the object that represents this extension's manifest.
	 * 
	 * @throws OdaException
	 */
	static ExtensionManifest getManifest() throws OdaException {
		return ManifestExplorer.getInstance().getExtensionManifest(ODA_DATA_SOURCE_ID);
	}

	/**
	 * Returns the native data type name of the specified code, as defined in this data source extension's manifest.
	 * 
	 * @param nativeTypeCode the native data type code
	 * @return corresponding native data type name
	 * @throws OdaException if lookup fails
	 */
	static String getNativeDataTypeName(int nativeDataTypeCode) throws OdaException {
		DataTypeMapping typeMapping = getManifest().getDataSetType(null).getDataTypeMapping(nativeDataTypeCode);
		if (typeMapping != null)
			return typeMapping.getNativeType();
		return "Non-defined";
	}

}
