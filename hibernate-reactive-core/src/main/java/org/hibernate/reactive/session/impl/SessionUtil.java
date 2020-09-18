/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.engine.spi.SessionImplementor;

import java.io.Serializable;

public class SessionUtil {

	public static void throwEntityNotFound(SessionImplementor session, String entityName, Serializable identifier) {
		session.getFactory().getEntityNotFoundDelegate().handleEntityNotFound( entityName, identifier );
	}

	public static void checkEntityFound(SessionImplementor session, String entityName, Serializable identifier, Object optional) {
		if ( optional==null ) {
			throwEntityNotFound(session, entityName, identifier);
		}
	}

}
