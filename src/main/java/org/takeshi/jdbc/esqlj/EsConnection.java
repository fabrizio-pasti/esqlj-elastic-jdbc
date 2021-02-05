package org.takeshi.jdbc.esqlj;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class EsConnection implements Connection {

	private RestHighLevelClient client;
	private EsMetaData esMetaData;

	private static RestHighLevelClient sharedClient;
	private static EsMetaData sharedEsMetaData;
	
	private boolean sharedConnection; 

	public EsConnection() throws SQLException {
		open();
		sharedConnection = Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_SHARED_CONNECTION, Boolean.class);
	}

	private void open() throws SQLException {
		if (Configuration.isTestMode()) {
			return;
		}
				
		openConnection();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return iface.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new EsStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return true;
	}

	@Override
	public void commit() throws SQLException {

	}

	@Override
	public void rollback() throws SQLException {

	}

	@Override
	public void close() throws SQLException {
		try {
			if (Configuration.isTestMode()) {
				return;
			}
			client.close();
			client = null;
		} catch (IOException e) {
			throw new SQLException("Failed to close: ".concat(e.getMessage()));
		}
	}

	@Override
	public boolean isClosed() throws SQLException {
		return !isOpen();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		if(client == null) {
			throw new SQLNonTransientConnectionException();
		}
		return esMetaData;
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
	}

	@Override
	public String getCatalog() throws SQLException {
		return null;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		if(resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY - CONCUR_READ_ONLY is supported");
		}
		return new EsStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return null;
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		if(resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY - CONCUR_READ_ONLY is supported");
		}
		return new EsPreparedStatement(this, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		if(autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
			throw new SQLFeatureNotSupportedException("Only NO_GENERATED_KEYS is supported");
		}
		return new EsPreparedStatement(this, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return isOpen();
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getSchema() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		close();
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		// not implemented
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return 0;
	}

	public RestHighLevelClient getElasticClient() {
		return client;
	}

	private void openConnection() throws SQLException {
		if((!isOpen() && !sharedConnection) || (!isOpen() && sharedConnection && !isSharedConnectionOpen())) {
			client = new RestHighLevelClient(RestClient.builder(Configuration.getUrls().stream()
					.map(esi -> new HttpHost(esi.getServer(), esi.getPort(), esi.getProtocol().name()))
					.toArray(HttpHost[]::new)));
			esMetaData = new EsMetaData(this);
			if(sharedConnection) {
				sharedClient = client;
				sharedEsMetaData = esMetaData;
			}
		} else if(!isOpen()) {
			client = sharedClient;
			esMetaData = sharedEsMetaData;
		}
	}
	
	public boolean isOpen() {
		try {
			return client != null && client.ping(RequestOptions.DEFAULT);
		} catch (IOException e) {
			return false;
		}
	}
	
	public boolean isSharedConnectionOpen() {
		try {
			return sharedClient != null && sharedClient.ping(RequestOptions.DEFAULT);
		} catch (IOException e) {
			return false;
		}
	}

}
