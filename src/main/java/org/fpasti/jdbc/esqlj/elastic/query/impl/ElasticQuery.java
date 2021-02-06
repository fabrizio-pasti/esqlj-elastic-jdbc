package org.fpasti.jdbc.esqlj.elastic.query.impl;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.EsResultSetMetaData;
import org.fpasti.jdbc.esqlj.elastic.query.AbstractQuery;
import org.fpasti.jdbc.esqlj.elastic.query.QueryType;
import org.fpasti.jdbc.esqlj.elastic.query.data.PageDataElastic;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestBuilder;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.model.PageDataState;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.support.ElasticUtils;
import org.fpasti.jdbc.esqlj.support.EsRuntimeException;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ElasticQuery extends AbstractQuery {

	private PageDataElastic pageData;
	private ResultSetMetaData resultSetMetaData;
	private int fetchSize;
	private boolean rsEmpty;
	private RequestInstance requestInstance;
	
	public ElasticQuery(EsConnection connection, SqlStatementSelect select) throws SQLException {
		super(connection, QueryType.SCROLLABLE, select.getIndex().getName());
		this.fetchSize = Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_QUERY_SCROLL_FETCH_SIZE, Integer.class);
		initialFetch(select);
	}
	
	private void initialFetch(SqlStatementSelect select) throws SQLException {
		try {
			requestInstance = RequestBuilder.buildRequest(getConnection(), select, fetchSize);
			
			pageData = new PageDataElastic(getSource(), requestInstance);
				
			SearchResponse searchResponse = getConnection().getElasticClient().search(requestInstance.getSearchRequest(), RequestOptions.DEFAULT);
			pageData.pushData(searchResponse);
			requestInstance.updateRequest(searchResponse, pageData);
			rsEmpty = pageData.isEmpty();
			clearScrollIfRequired(searchResponse);
		} catch(EsRuntimeException ere) {
			throw new SQLSyntaxErrorException(ere.getMessage());
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
	}

	@SuppressWarnings("incomplete-switch")
	private void scrollFetch() throws SQLException {
		try {
			switch(requestInstance.getPaginationMode()) {
				case SCROLL_API:
					paginateByScrollApi();
					break;
				case BY_ORDER:
				case BY_ORDER_WITH_PIT:
					paginateByOrder();
					break;
			}
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
	}

	private void paginateByScrollApi() throws IOException, SQLException {
		SearchScrollRequest scrollRequest = new SearchScrollRequest(requestInstance.getPaginationId());
		scrollRequest.scroll(TimeValue.timeValueMinutes(Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class)));
		SearchResponse searchResponse = getConnection().getElasticClient().scroll(scrollRequest, RequestOptions.DEFAULT);
		pageData.pushData(searchResponse);
		requestInstance.updateRequest(searchResponse, pageData);
		clearScrollIfRequired(searchResponse);
	}

	private void paginateByOrder() throws IOException, SQLException {
		SearchResponse searchResponse = getConnection().getElasticClient().search(requestInstance.getSearchRequest(), RequestOptions.DEFAULT);
		pageData.pushData(searchResponse);
		requestInstance.updateRequest(searchResponse, pageData);
		rsEmpty = pageData.isEmpty();
		clearScrollIfRequired(searchResponse);
	}

	private void clearScrollIfRequired(SearchResponse response) throws SQLException {
		if(response.getHits().getHits().length < fetchSize) {
			clearScroll();
		}
	}
	
	@SuppressWarnings("incomplete-switch")
	private void clearScroll() throws SQLException {
		if(!requestInstance.isScrollOpen()) {
			return;
		}
		
		try {
			requestInstance.setScrollOpen(false);
			switch(requestInstance.getPaginationMode()) {
				case SCROLL_API:
					clearPaginationByScrollApi();
					break;
				case BY_ORDER_WITH_PIT:
					ElasticUtils.deletePointInTime(getConnection(), requestInstance.getPaginationId());
					break; 
			}
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
	}

	private void clearPaginationByScrollApi() throws IOException {
		ClearScrollRequest clearScrollRequest = new ClearScrollRequest(); 
		clearScrollRequest.addScrollId(requestInstance.getPaginationId());
		getConnection().getElasticClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
	}

	@Override
	public boolean next() throws SQLException {
		if(requestInstance.isScrollable() && requestInstance.isScrollOpen() && pageData.oneRowLeft()) {
			scrollFetch();
		}
		switch(pageData.next()) {
			case ITERATION_FINISHED:
				return false;
			case NOT_INITIALIZED:
				throw new SQLException("Query not initialized");
			default:
				return true;		
		}
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
		this.fetchSize = size;
	}

	@Override
	public int getFetchSize() {
		return fetchSize; 
	}

	@Override
	public boolean isForwardOnly() {
		return false;
	}

	@Override
	public void close() throws SQLException {
		clearScroll();
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
			resultSetMetaData = new EsResultSetMetaData(getSource(), requestInstance.getFieldNames(), requestInstance.getColumnNames(), requestInstance.getFieldTypes());
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
		return rsEmpty;
	}

}
