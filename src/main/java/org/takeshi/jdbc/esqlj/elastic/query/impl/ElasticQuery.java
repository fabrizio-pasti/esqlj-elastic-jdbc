package org.takeshi.jdbc.esqlj.elastic.query.impl;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.takeshi.jdbc.esqlj.Configuration;
import org.takeshi.jdbc.esqlj.ConfigurationEnum;
import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.EsMetaData;
import org.takeshi.jdbc.esqlj.EsResultSetMetaData;
import org.takeshi.jdbc.esqlj.elastic.query.AbstractQuery;
import org.takeshi.jdbc.esqlj.elastic.query.QueryType;
import org.takeshi.jdbc.esqlj.elastic.query.data.PageDataElastic;
import org.takeshi.jdbc.esqlj.elastic.query.impl.search.RequestBuilder;
import org.takeshi.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.takeshi.jdbc.esqlj.elastic.query.model.PageDataState;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

public class ElasticQuery extends AbstractQuery {

	private ParsedQuery parsedQuery;
	private PageDataElastic pageData;
	private boolean scrollable = true;
	private ResultSetMetaData resultSetMetaData;
	private int fetchSize;
	private boolean rsEmpty;
	private RequestInstance requestInstance;
	
	public ElasticQuery(EsConnection connection, ParsedQuery query, boolean scrollable) throws SQLException {
		super(connection, QueryType.SCROLLABLE, query.getIndex().getName());
		this.parsedQuery = query;
		this.scrollable = scrollable;
		this.fetchSize = Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_FETCH_SIZE, Integer.class);
		initialFetch();
	}

	private void initialFetch() throws SQLException {
		try {
			requestInstance = RequestBuilder.buildRequest(((EsMetaData)getConnection().getMetaData()).getMetaDataService().getIndexMetaData(getSource()), parsedQuery, fetchSize, scrollable);
			
			pageData = new PageDataElastic(getSource(), requestInstance, true);
				
			SearchResponse searchResponse = getConnection().getElasticClient().search(requestInstance.getSearchRequest(), RequestOptions.DEFAULT);
			pageData.pushData(searchResponse);
			rsEmpty = pageData.isEmpty();
			clearScrollIfRequired(searchResponse);
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
	}

	private void scrollFetch() throws SQLException {
		try {
			SearchScrollRequest scrollRequest = new SearchScrollRequest(pageData.getScrollId());
			scrollRequest.scroll(TimeValue.timeValueMinutes(Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class)));
			SearchResponse searchResponse = getConnection().getElasticClient().scroll(scrollRequest, RequestOptions.DEFAULT);
			pageData.pushData(searchResponse);
			clearScrollIfRequired(searchResponse);
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	private void clearScrollIfRequired(SearchResponse response) throws SQLException {
		if(response.getHits().getHits().length < fetchSize) {
			clearScroll();
		}
	}
	
	private void clearScroll() throws SQLException {
		try {
			open = false;
			if(pageData.getScrollId() != null) {
				ClearScrollRequest clearScrollRequest = new ClearScrollRequest(); 
				clearScrollRequest.addScrollId(pageData.getScrollId());
				getConnection().getElasticClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
			}
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
	}

	@Override
	public boolean next() throws SQLException {
		if(scrollable && open && pageData.oneRowLeft()) {
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
			resultSetMetaData = new EsResultSetMetaData(getSource(), requestInstance.getFieldNames(), requestInstance.getFieldTypes());
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
