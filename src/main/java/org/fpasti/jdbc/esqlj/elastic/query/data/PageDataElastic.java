package org.fpasti.jdbc.esqlj.elastic.query.data;

import java.sql.SQLException;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.metrics.ParsedValueCount;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticObject;
import org.fpasti.jdbc.esqlj.elastic.model.EsGeoPoint;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.model.DataRow;
import org.fpasti.jdbc.esqlj.elastic.query.model.PageDataState;
import org.fpasti.jdbc.esqlj.support.SimpleDateFormatThreadSafe;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class PageDataElastic {
	
	private PageDataState state = PageDataState.NOT_INITIALIZED;
	private int currentIdxCurrentRow = -1;
	private int iterationStep = 1;
	private RequestInstance req;
	private Long fetchedRows = 0L;
	
	private List<DataRow> dataRows;

	public static final SimpleDateFormatThreadSafe sdfTimestamp = new SimpleDateFormatThreadSafe("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", TimeZone.getTimeZone("UTC"));
	
	public PageDataElastic(String source, RequestInstance req) {
		this.req = req;
	}

	public void pushData(SearchResponse searchResponse) {
		currentIdxCurrentRow = -1;
		
		switch(req.getSelect().getQueryType()) {
			case DOCS:
				pushDocuments(searchResponse);
				break;
			case AGGR_COUNT_ALL:
				manageCountAll(searchResponse);
				break;
			case AGGR_COUNT_FIELD:
				manageCountField(searchResponse);
				break;
			case AGGR_GROUPED:
				break;
		}
	}

	private void pushDocuments(SearchResponse res) {
		boolean firstPush = dataRows == null;
			
		if(!firstPush) {
			DataRow dataRow = dataRows.get(dataRows.size() - 1);
			dataRows = new ArrayList<DataRow>();
			dataRows.add(dataRow);
		} else {
			dataRows = new ArrayList<DataRow>();
		}
		
		int takeNRows = req.getSelect().getLimit() != null ? (res.getHits().getHits().length + fetchedRows > req.getSelect().getLimit() ? new Long(req.getSelect().getLimit() - fetchedRows).intValue() : res.getHits().getHits().length) : res.getHits().getHits().length;

		for(int i = 0; i < takeNRows; i++) {
			SearchHit searchHit = res.getHits().getHits()[i];
			List<Object> data = new ArrayList<Object>();
			req.getFields().forEach((name, field) -> {
				if(field.isDocValue()) {
					DocumentField docField = searchHit.field(field.getFullName());
					if(docField != null) {
						data.add(resolveField(field, docField.getValue())); // only first field value is managed
					} else {
						data.add(null);
					}
				} else if(field.isSourceField() && req.isSourceFieldsToRetrieve()) {
					data.add(searchHit.getSourceAsMap().get(field.getFullName()));
				} else if(field.getFullName().equals(ElasticObject.DOC_ID_ALIAS)) {
					data.add(searchHit.getId());
				} else if(field.getFullName().equals(ElasticObject.DOC_SCORE)) {
					data.add(searchHit.getScore());
				} else {
					data.add(null);
				}
			});
			dataRows.add(new DataRow(data));			
		}
		
		fetchedRows += dataRows.size() - (firstPush ? 0 : 1) ;
		state = state == PageDataState.NOT_INITIALIZED ? PageDataState.READY_TO_ITERATE : PageDataState.ITERATION_STARTED;
	}
	
	private void manageCountAll(SearchResponse res) {
		dataRows = new ArrayList<DataRow>();
		List<Object> data = new ArrayList<Object>();
		data.add(res.getHits().getTotalHits().value);
		dataRows.add(new DataRow(data));
		fetchedRows = new Long(dataRows.size());
		state = PageDataState.READY_TO_ITERATE;
	}
	
	private void manageCountField(SearchResponse res) {
		dataRows = new ArrayList<DataRow>();
		List<Object> data = new ArrayList<Object>();
		
		res.getAggregations().asList().forEach(aggregation -> {
			ParsedValueCount valueCount = (ParsedValueCount)aggregation;
			data.add(valueCount.getValue());
		});
		
		dataRows.add(new DataRow(data));
		fetchedRows = new Long(dataRows.size());
		state = PageDataState.READY_TO_ITERATE;
	}
	
	private Object resolveField(ElasticObject elObject, Object value) {
		value = resolveType(elObject, value);
		
		if(elObject.getLinkedQueryColumn() != null) {
			if(elObject.getLinkedQueryColumn().getFormatter() != null) {
				return elObject.getLinkedQueryColumn().getFormatter().resolveValue(value);
			}
		}
		
		return value;
	}

	private Object resolveType(ElasticObject elObject, Object value) {
		switch(elObject.getType()) {
			case BOOLEAN:
				if(value != null) {
					return value;
				}
				return null;
			case GEO_POINT:
				return resolveGeoPoint(value);
			case DATE:
			case DATE_NANOS:
				try {
					return sdfTimestamp.parse((String)value).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
				} catch (ParseException e) {
					// log error
					return null;
				}
			default:
				return value;
		}
	}

	private Object resolveGeoPoint(Object value) {
		if(value == null) {
			return null;
		}
		
		String[] gp = ((String)value).split(",");
		return new EsGeoPoint(Double.parseDouble(gp[0]), Double.parseDouble(gp[1]));
		
	}

	public DataRow getCurrentRow() throws SQLException {
		switch (getState()) {
			case NOT_INITIALIZED:
				throw new SQLException("PageData not initialized");
			case READY_TO_ITERATE:
				throw new SQLException("PageData not started");
			default:
				return dataRows.get(currentIdxCurrentRow);
		}
	}

	public boolean isReadyOrStarted() {
		switch (getState()) {
			case READY_TO_ITERATE:
			case ITERATION_STARTED:
				return true;
			default:
				return false;
		}
	}

	public boolean isProvidingData() {
		switch (getState()) {
			case NOT_INITIALIZED:
			case READY_TO_ITERATE:
				return false;
			default:
				return true;
		}
	}

	public boolean isBeforeFirst() {
		return state == PageDataState.NOT_INITIALIZED || state == PageDataState.READY_TO_ITERATE;
	}

	public boolean isFirst() {
		if (isReadyOrStarted()) {
			return currentIdxCurrentRow == 0;
		}
		return false;
	}

	public Object getColumnValue(String columnName) throws SQLException {
		return getCurrentRow().data.get(req.getFieldNames().indexOf(columnName));
	}

	@SuppressWarnings("unchecked")
	public <T> T getColumnValue(String columnName, Class<T> clazz) throws SQLException { // todo: convert type if required													// required
		return (T) getCurrentRow().data.get(req.getFieldNames().indexOf(columnName));
	}

	public Object getColumnValue(int columnIndex) throws SQLException {
		return getCurrentRow().data.get(columnIndex);
	}

	@SuppressWarnings("unchecked")
	public <T> T getColumnValue(int columnIndex, Class<T> clazz) throws SQLException { // todo: convert type if required
		return (T) getCurrentRow().data.get(columnIndex);
	}
	public void setColumnValue(String columnName, Object data) throws SQLException {
		
		getCurrentRow().put(req.getFieldNames().indexOf(columnName), data);
	}

	public PageDataState next() throws SQLException {
		switch (getState()) {
			case NOT_INITIALIZED:
				throw new SQLException("PageData not initialized");
			case READY_TO_ITERATE:
			case ITERATION_STARTED:
				this.state = doNext();
				return this.state;
			default:
				return getState();
		}
	}

	public void clear() {
		dataRows = new ArrayList<DataRow>();
		currentIdxCurrentRow = 0;
		state = PageDataState.NOT_INITIALIZED;
	}

	public void reset() throws SQLException {
		if (iterationStep > 0) {
			moveToRow(0);
			state = PageDataState.READY_TO_ITERATE;
		} else {
			moveToRow(dataRows.size() - 1);
		}
	}

	public void moveToLast() throws SQLException {
		if (iterationStep > 0) {
			moveToRow(dataRows.size() - 1);
		} else {
			moveToRow(0);
			state = PageDataState.READY_TO_ITERATE;
		}
	}

	public void moveToRow(int rowIndex) throws SQLException {
		switch (getState()) {
			case NOT_INITIALIZED:
				throw new SQLException("PageData not initialized");
			case READY_TO_ITERATE:
				throw new SQLException("PageData not started");
			default:
				if (rowIndex >= dataRows.size()) {
					throw new SQLException(String.format("Row %d does not exists on resultset", rowIndex));
				}
				currentIdxCurrentRow = rowIndex - 1;
				if (currentIdxCurrentRow == dataRows.size() - 1) {
					state = PageDataState.ITERATION_FINISHED;
				} else {
					state = PageDataState.ITERATION_STARTED;
				}

				break;
		}
	}

	public void moveByDelta(int rows) throws SQLException {
		int newIndex = currentIdxCurrentRow + rows;
		if (newIndex < 0 || newIndex >= dataRows.size()) {
			throw new SQLException(String.format("Row %d is out of current resultset range", newIndex));
		}

		moveToRow(newIndex);
	}

	public void finish() {
		state = PageDataState.ITERATION_FINISHED;
	}

	public int getSize() {
		return dataRows.size();
	}

	public int getCurrentRowIndex() {
		return currentIdxCurrentRow;
	}

	public PageDataState getState() {
		return state;
	}

	public void setIterationStep(int iterationStep) {
		this.iterationStep = iterationStep;
	}

	public int getColumnIndex(String columnLabel) {
		return req.getFieldNames().indexOf(columnLabel);
	}

	private PageDataState doNext() {
		if (dataRows.size() >= currentIdxCurrentRow) {
			currentIdxCurrentRow += iterationStep;
			return currentIdxCurrentRow == dataRows.size() ? PageDataState.ITERATION_FINISHED : PageDataState.ITERATION_STARTED;
		}

		return PageDataState.ITERATION_FINISHED;
	}

	public boolean oneRowLeft() {
		return currentIdxCurrentRow == dataRows.size() - 2;
	}

	public boolean isEmpty() {
		return dataRows.isEmpty();
	}

	public Long getFetchedRows() {
		return fetchedRows;
	}

	

}
