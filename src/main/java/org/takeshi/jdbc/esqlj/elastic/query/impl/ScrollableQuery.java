package org.takeshi.jdbc.esqlj.elastic.query.impl;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.takeshi.jdbc.esqlj.Configuration;
import org.takeshi.jdbc.esqlj.ConfigurationEnum;
import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.EsMetaData;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.model.IndexMetaData;
import org.takeshi.jdbc.esqlj.elastic.query.AbstractQuery;
import org.takeshi.jdbc.esqlj.elastic.query.QueryType;
import org.takeshi.jdbc.esqlj.elastic.query.data.PageDataElastic;
import org.takeshi.jdbc.esqlj.elastic.query.model.PageDataState;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

public class ScrollableQuery extends AbstractQuery {

	private ParsedQuery parsedQuery;
	private PageDataElastic pageData;
	private IndexMetaData indexMetaData;
	
	public ScrollableQuery(EsConnection connection, ParsedQuery query) throws SQLException {
		super(connection, QueryType.SCROLLABLE, query.getIndex().getName());
		this.parsedQuery = query;
		this.indexMetaData = ((EsMetaData)connection.getMetaData()).getMetaDataService().getIndexMetaData(getSource());
		pageData = new PageDataElastic(getSource(), indexMetaData, true);
		
		fetchData();
	}

	private void fetchData() throws SQLException {
		try {
			SearchRequest searchRequest = new SearchRequest(getSource());
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			List<String> srcIncludeFields = Configuration.getConfiguration(ConfigurationEnum.CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT, Boolean.class) ?  new ArrayList<String>() : null;
			
			indexMetaData.getFields().stream().forEach(field -> {
				if(field.getType().equals(ElasticFieldType.TEXT)) {
					if(srcIncludeFields != null) {
						srcIncludeFields.add(field.getFullName());
					}
				} else {
					searchSourceBuilder.docValueField(field.getFullName());
				}
			});
	
			searchSourceBuilder.size(Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_FETCH_SIZE, Integer.class)); 
			searchRequest.source(searchSourceBuilder);
			searchRequest.scroll(TimeValue.timeValueMinutes(Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class))); 
			SearchResponse searchResponse = getConnection().getElasticClient().search(searchRequest, RequestOptions.DEFAULT);
			pageData.pushData(searchResponse);
			String scrollId = searchResponse.getScrollId();
			DocumentField field = searchResponse.getHits().getAt(0).field("field2");
			Object field1 = searchResponse.getHits().getAt(0).getSourceAsMap().get("field1");
			SearchHits hits = searchResponse.getHits();
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
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
	public void setFetchSize() {
		// TODO Auto-generated method stub
		
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
		return pageData.getResultSetMetaData();
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

}
