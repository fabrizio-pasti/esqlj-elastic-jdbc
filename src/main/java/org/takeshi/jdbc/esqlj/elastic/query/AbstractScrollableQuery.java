package org.takeshi.jdbc.esqlj.elastic.query;

import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.takeshi.jdbc.esqlj.EsConnection;

public class AbstractScrollableQuery extends AbstractQuery {

	public AbstractScrollableQuery(EsConnection connection, String source) {
		super(connection, QueryType.SCROLLABLE, source);
	}
	
	public AbstractScrollableQuery(EsConnection connection, String source, String... columnsName) {
		super(connection, QueryType.SCROLLABLE, source, columnsName);
	}

	@Override
	public boolean next() {
		return false;
	}

	@Override
	public boolean isFirst() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isLast() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void reset() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException(); 
		
	}

	@Override
	public void finish() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean moveToFirst() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getCurrentRowIndex() throws SQLException {
		return 0;
	}

	@Override
	public boolean moveToLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean moveToRow(int rowIndex) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isProvidingData() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean moveByDelta(int rows) {
		return false;
	}

	@Override
	public void setIterationStep(int iterationStep) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setFetchSize(int rows) {
		
	}

	@Override
	public int getFetchSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isForwardOnly() {
		return true;
	}

	@Override
	public void close() throws SQLException {
		setClosed();
	}

	@Override
	public <T> T getColumnValue(int columnIndex, Class<T> type) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getColumnValue(String columnName, Class<T> type) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSetMetaData getResultSetMetaData() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getColumnValue(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getColumnValue(String columnName) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int findColumnIndex(String columnLabel) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isBeforeFirst() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RowId getRowId() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}


}
