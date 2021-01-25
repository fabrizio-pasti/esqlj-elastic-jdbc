package org.takeshi.jdbc.esqlj.elastic.query;

import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.takeshi.jdbc.esqlj.EsConnection;

public abstract class AbstractQuery {
	private EsConnection connection;
	private QueryType queryType;
	private List<String> columnsName;
	private boolean open = true;
	private String source;
	
	public AbstractQuery(EsConnection connection, QueryType queryType, String source, String... columnsName) {
		this.source = source;
		this.queryType = queryType;
		this.connection = connection;
		this.columnsName = Arrays.asList(columnsName);
	}
	
	public abstract boolean next() throws SQLException;
	
	protected EsConnection getConnection() {
		return connection;
	}

	public List<String> getColumnsName() {
		return columnsName;
	}	
	
	public abstract boolean isFirst() throws SQLException;
	
	public abstract boolean isLast() throws SQLException;
	
	public abstract void reset() throws SQLException;
	
	public abstract void finish() throws SQLException;

	public abstract boolean moveToFirst() throws SQLException;
	
	public abstract boolean moveToLast() throws SQLException;
	
	public abstract int getCurrentRowIndex() throws SQLException;

	public abstract boolean moveToRow(int rowIndex) throws SQLException;
	
	public abstract boolean isProvidingData()  throws SQLException;

	public abstract boolean moveByDelta(int rows) throws SQLException;

	public abstract void setIterationStep(int iterationStep);

	public abstract void setFetchSize(int rows);

	public abstract int getFetchSize();

	public abstract boolean isForwardOnly();

	public abstract void close() throws SQLException;
	
	public boolean isOpen() {
		return open;
	}
	
	protected void setClosed() {
		open = false;
	}

	public abstract Object getColumnValue(int columnIndex) throws SQLException;
	
	public abstract <T> T getColumnValue(int columnIndex, Class<T> type) throws SQLException;
	
	public abstract Object getColumnValue(String columnName) throws SQLException;
	
	public abstract <T> T getColumnValue(String columnName, Class<T> type) throws SQLException;

	public abstract ResultSetMetaData getResultSetMetaData();

	public abstract RowId getRowId() throws SQLException;
	
	public String getSource() {
		return source;
	}

	public abstract int findColumnIndex(String columnLabel);

	public abstract boolean isBeforeFirst();

	public QueryType getQueryType() {
		return queryType;
	}

	public abstract boolean isEmpty();
	
}
