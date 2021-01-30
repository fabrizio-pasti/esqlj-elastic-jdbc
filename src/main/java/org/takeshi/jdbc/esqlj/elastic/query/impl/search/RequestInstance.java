package org.takeshi.jdbc.esqlj.elastic.query.impl.search;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.takeshi.jdbc.esqlj.Configuration;
import org.takeshi.jdbc.esqlj.ConfigurationEnum;
import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.EsMetaData;
import org.takeshi.jdbc.esqlj.elastic.metadata.ElasticServerDetails;
import org.takeshi.jdbc.esqlj.elastic.metadata.MetaDataService;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticField;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.model.IndexMetaData;
import org.takeshi.jdbc.esqlj.elastic.query.model.PaginationType;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

public class RequestInstance {

	private Map<String, ElasticField> fields;
	private SearchRequest searchRequest;
	private SearchSourceBuilder searchSourceBuilder;
	private int fetchSize;
	private ParsedQuery query;
	private PaginationType paginationMode = PaginationType.NO_SCROLL;
	private String paginationId;
	private MetaDataService metaDataService;
	private IndexMetaData indexMetaData;
	private boolean pointInTimeApiAvailable;
	private Object[] paginationSortValues;
	private boolean scrollOpen;
	
	Pattern pattern = Pattern.compile("\"id\":\\s*\"([\\w=]*)\"");
	
	public RequestInstance(EsConnection connection, int fetchSize, ParsedQuery query) throws SQLException {
		this.metaDataService = ((EsMetaData)connection.getMetaData()).getMetaDataService();
		this.query = query;
		this.indexMetaData = metaDataService.getIndexMetaData(query.getIndex().getName());
		searchRequest = new SearchRequest(query.getIndex().getName());
		searchSourceBuilder = new SearchSourceBuilder();
		this.fetchSize = fetchSize;
		implementScrollStrategy();
		checkPointInTimeWorkAround(connection); // hey Elastic team! Where is the api for point in time search?
	}

	public IndexMetaData getIndexMetaData() {
		return indexMetaData;
	}
	
	public Map<String, ElasticField> getFields() {
		return fields;
	}

	public void setFields(Map<String, ElasticField> fields) {
		this.fields = fields;
	}

	public List<String> getFieldNames() {
		return fields.keySet().stream().collect(Collectors.toList());
	}

	public List<ElasticFieldType> getFieldTypes() {
		return fields.entrySet().stream().map(field -> field.getValue().getType()).collect(Collectors.toList());
	}

	public SearchRequest getSearchRequest() {
		return searchRequest;
	}

	public SearchSourceBuilder getSearchSourceBuilder() {
		return searchSourceBuilder;
	}

	public boolean isStarSelect() {
		return query.getFields().size() == 1 && query.getFields().get(0).getName().equals("*");
	}
	
	public boolean isSourceFieldsToRetrieve() {
		return !isStarSelect() || Configuration.getConfiguration(ConfigurationEnum.CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT, Boolean.class);
	}

	public PaginationType getPaginationMode() {
		return paginationMode;
	}
	
	public boolean isScrollable() {
		return !paginationMode.equals(PaginationType.NO_SCROLL);
	}
	
	public boolean isOrdered() {
		return true;
	}
		
	public int getFetchSize() {
		return fetchSize;
	}
	
	public String getPaginationId() {
		return paginationId;
	}

	public void setPaginationId(String paginationId) {
		this.paginationId = paginationId;
	}

	private void implementScrollStrategy() {
		if(query.getLimit() != null && query.getLimit() < Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_SCROLL_FROM_ROWS, Long.class)) {
			return;
		}
		
		if(isOrdered() && !Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_SCROLL_ONLY_BY_SCROLL_API, Boolean.class)) {
			paginationMode = metaDataService.getElasticServerDetails().isElasticReleaseEqOrGt(ElasticServerDetails.ELASTIC_REL_7_10_0) && pointInTimeApiAvailable ? PaginationType.BY_ORDER_WITH_PIT : PaginationType.BY_ORDER;
		} else {
			paginationMode = PaginationType.SCROLL_API;
		}
		
		setScrollOpen(true);
	}

	@SuppressWarnings("incomplete-switch")
	public void updatePagination(SearchResponse searchResponse) throws SQLNonTransientConnectionException {
		switch(paginationMode) {
			case SCROLL_API:
				paginationId = searchResponse.getScrollId();
				break;
			case BY_ORDER:
				updateSearchAfter(searchResponse);
				break;
			case BY_ORDER_WITH_PIT:
				updateSearchAfter(searchResponse);
				PointInTimeBuilder pit = new PointInTimeBuilder(searchResponse.pointInTimeId());
				pit.setKeepAlive(TimeValue.timeValueMinutes(Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class)));
				getSearchSourceBuilder().pointInTimeBuilder(pit);
				break;
		}
	}

	private void updateSearchAfter(SearchResponse searchResponse) {
		if(searchResponse.getHits().getHits().length > 0) {
			paginationSortValues = searchResponse.getHits().getAt(searchResponse.getHits().getHits().length - 1).getSortValues();
			searchSourceBuilder.searchAfter(paginationSortValues);
		}
	}
	
	private void checkPointInTimeWorkAround(EsConnection connection) {
		try {
			connection.getElasticClient().migration();
		} catch(Exception e) {
			pointInTimeApiAvailable = true;
		}
	}

	public boolean isScrollOpen() {
		return scrollOpen;
	}

	public void setScrollOpen(boolean scrollOpen) {
		this.scrollOpen = scrollOpen;
	}

}
