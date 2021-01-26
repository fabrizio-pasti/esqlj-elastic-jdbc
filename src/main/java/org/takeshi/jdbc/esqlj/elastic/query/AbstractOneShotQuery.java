package org.takeshi.jdbc.esqlj.elastic.query;

import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.EsResultSetMetaData;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.query.data.PageDataArray;
import org.takeshi.jdbc.esqlj.elastic.query.model.DataRow;
import org.takeshi.jdbc.esqlj.elastic.query.model.PageDataState;

public class AbstractOneShotQuery extends AbstractQuery {

	private PageDataArray pageData;
	private ResultSetMetaData resultSetMetaData;
	
	public AbstractOneShotQuery(EsConnection connection, String source, String... columnNames) {
		super(connection, QueryType.STATIC, source, columnNames);
		pageData = new PageDataArray(getColumnNames());
	}

	@Override
	public boolean next() throws SQLException {
		switch(pageData.next()) {
		case ITERATION_FINISHED:
			return false;
		case NOT_INITIALIZED:
			throw new SQLException("Query not initialized");
		default:
			return true;		
		}
	}
	
	public void populate(List<List<Object>> data) {
		pageData.populate(data);
	}
	
	public void insertRow(Object... values) {
		pageData.push(values);
	}

	public void insertRowWithData(Map<String, Object> data) {
		pageData.pushSubsetData(data);
	}

	@Override
	public boolean isBeforeFirst() {
		return pageData.isBeforeFirst();
	}

	@Override
	public boolean isFirst() {
		return pageData.isFirst();
	}
	
	@Override
	public boolean isLast() {
		return pageData.getState() == PageDataState.ITERATION_FINISHED;
	}

	@Override
	public void reset() throws SQLException {
		pageData.reset();
	}

	@Override
	public void finish() throws SQLException {
		pageData.finish();
	}

	@Override
	public boolean moveToFirst() throws SQLException {
		pageData.reset();
		return pageData.getSize() > 0;
	}

	@Override
	public boolean moveToLast() throws SQLException {
		pageData.moveToLast();
		return pageData.getSize() > 0;
	}

	@Override
	public int getCurrentRowIndex() throws SQLException {
		return pageData.getCurrentRowIndex() + 1;
	}
	
	@Override
	public boolean moveToRow(int rowIndex) throws SQLException {
		pageData.moveToRow(rowIndex - 1);
		return pageData.getSize() > 0;
	}

	@Override
	public boolean isProvidingData() {
		return pageData.isProvidingData();
	}

	@Override
	public boolean moveByDelta(int rows) throws SQLException {
		if(!isProvidingData()) {
			return false;
		}
		pageData.moveByDelta(rows);
		return true;
	}

	@Override
	public void setIterationStep(int iterationStep) {
		pageData.setIterationStep(iterationStep);
	}

	@Override
	public void setFetchSize(int size) {
		
	}

	@Override
	public int getFetchSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isForwardOnly() {
		return false;
	}

	@Override
	public void close() throws SQLException {
		pageData = null;
		setClosed();
	}

	@Override
	public <T> T getColumnValue(int columnIndex, Class<T> type) throws SQLException {
		return pageData.getColumnValue(columnIndex - 1, type);
	}

	@Override
	public <T> T getColumnValue(String columnName, Class<T> type) throws SQLException {
		return pageData.getColumnValue(columnName, type);
	}

	
	@Override
	public ResultSetMetaData getResultSetMetaData() {
		if(resultSetMetaData == null) {
			resultSetMetaData = new EsResultSetMetaData(getSource(), getColumnNames(), fetchTypesByData(getColumnNames(), pageData.getDataRows()));
		}
		return resultSetMetaData;
	}

	@Override
	public Object getColumnValue(int columnIndex) throws SQLException {
		return pageData.getColumnValue(columnIndex - 1);
	}

	@Override
	public Object getColumnValue(String columnName) throws SQLException {
		return pageData.getColumnValue(columnName);
	}

	@Override
	public int findColumnIndex(String columnLabel) {
		return pageData.getColumnIndex(columnLabel);
	}

	@Override
	public RowId getRowId() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isEmpty() {
		return pageData.isEmpty();
	}

	private List<ElasticFieldType> fetchTypesByData(List<String> columnNames, List<DataRow> dataRows) {
		List<ElasticFieldType> columnTypes = new ArrayList<ElasticFieldType>();
		
		if(dataRows == null || dataRows.size() == 0) {
			columnTypes = IntStream.range(0, columnNames.size()).mapToObj(i -> ElasticFieldType.UNKNOWN).collect(Collectors.toList());
		} else {
			columnTypes = IntStream.range(0, columnNames.size()).mapToObj(i -> {
				ElasticFieldType t = ElasticFieldType.UNKNOWN;
				for(int row = 0; row < dataRows.size(); row++) {
					if(dataRows.get(row).data.get(i) != null) {
						t = ElasticFieldType.resolveByValue(dataRows.get(row).data.get(i));
						break;
					}
				}
				return t;
			}).collect(Collectors.toList());
		}
		
		return columnTypes;
	}
}
