/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.testcontainers.containers.MSSQLServerContainer;

class SqlServerDatabase implements TestableDatabase {

	public static SqlServerDatabase INSTANCE = new SqlServerDatabase();

	public final static String IMAGE_NAME = "mcr.microsoft.com/mssql/server:2019-latest";

	/**
	 * Holds configuration for the SQL Server database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	static final MSSQLServerContainer<?> sqlServer = new MSSQLServerContainer<>( IMAGE_NAME )
		      .withUsername(DatabaseConfiguration.USERNAME)
		      .withPassword(DatabaseConfiguration.PASSWORD)
		      .withDatabaseName(DatabaseConfiguration.DB_NAME)
		      .withReuse(true);

	private String getRegularJdbcUrl() {
		return "jdbc:sqlserver://localhost:1433";
	}

	@Override
	public String getJdbcUrl() {
		return buildJdbcUrlWithCredentials( address() );
	}

	@Override
	public String getUri() {
		return buildUriWithCredentials( address() );
	}

	private String address() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			sqlServer.start();
			return sqlServer.getJdbcUrl();
		}

		return getRegularJdbcUrl();
	}

	private static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + ";user=" + sqlServer.getUsername() + ";password=" + sqlServer.getPassword();
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return jdbcUrl + ";" + sqlServer.getUsername() + ";password=" + sqlServer.getPassword();
	}

	private SqlServerDatabase() {
	}

}
