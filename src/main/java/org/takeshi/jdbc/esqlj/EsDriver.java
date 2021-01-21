package org.takeshi.jdbc.esqlj;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.takeshi.jdbc.esqlj.support.EsConfig;

public class EsDriver implements Driver {
	static {
		try {
			DriverManager.registerDriver(new EsDriver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// ESQLJ_CONNECTION_STRING
	// jdbc:esqlj://153.77.137.170:9200
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		EsConfig.parseConnectionString(url, info);
		return new EsConnection();
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith("jdbc:esqlj:");
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return EsConfig.getDriverPropertyInfo();
	}

	@Override
	public int getMajorVersion() {
		return EsConfig.getMajorVersion();
	}

	@Override
	public int getMinorVersion() {
		return EsConfig.getMinorVersion();
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

}
