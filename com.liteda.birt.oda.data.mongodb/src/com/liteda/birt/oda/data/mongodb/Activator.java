/*
 * Copyright (c) 2013 UAB Liteda (Ltd.)
 * All Rights Reserved. This source code is the confidential property of UAB Liteda.  All proprietary rights,
 * including but not limited to any trade secret, copyright, patent or trademark rights in and related to this
 * source code are the property of UAB Liteda.  This source code is not to be used, disclosed or reproduced in
 * any form without the express written consent of UAB Liteda.
 */
package com.liteda.birt.oda.data.mongodb;

import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.ui.statushandlers.StatusManager;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * @author dmitrijz
 */
public class Activator implements BundleActivator {

	public static final String PLUGIN_ID = "com.liteda.birt.oda.data.mongodb"; //$NON-NLS-1$

	private static BundleContext context;

	static BundleContext getContext() {
		return context;
	}

	/** {@inheritDoc} */
	@Override
	public void start(BundleContext bundleContext) throws Exception {
		Activator.context = bundleContext;
	}

	/** {@inheritDoc} */
	@Override
	public void stop(BundleContext bundleContext) throws Exception {
		Activator.context = null;
	}

	public static void log(int severity, String message, Exception e, Object... args) {
		Status status = new Status(severity, PLUGIN_ID, String.format(message, args), e);
		StatusManager.getManager().handle(status, StatusManager.LOG);
	}

	public static void logError(Exception e, String message, Object... args) {
		log(IStatus.ERROR, message, e, args);
	}

}
