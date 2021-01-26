package org.takeshi.jdbc.esqlj.elastic.query.impl.search;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.takeshi.jdbc.esqlj.Configuration;
import org.takeshi.jdbc.esqlj.ConfigurationEnum;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticField;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.model.IndexMetaData;
import org.takeshi.jdbc.esqlj.elastic.query.data.PageDataElastic;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

public class RequestInstance {

	private Map<String, ElasticField> fields;
	private SearchRequest searchRequest;
	private SearchSourceBuilder searchSourceBuilder;
	private int fetchSize;
	private boolean scrollable;
	private ParsedQuery query;
	
	public RequestInstance(IndexMetaData indexMetaData, int fetchSize, boolean scrollable, ParsedQuery query) {
		searchRequest = new SearchRequest(indexMetaData.getIndex());
		searchSourceBuilder = new SearchSourceBuilder();
		this.scrollable = scrollable;
		this.fetchSize = fetchSize;
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

	public boolean isSourceFieldsToRetrieve() {
		return (query.getFields() != null && query.getFields().size() > 0) || Configuration.getConfiguration(ConfigurationEnum.CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT, Boolean.class);
	}
		
	public void build() {
		searchSourceBuilder.size(fetchSize); 
		searchRequest.source(searchSourceBuilder);
		
		if(scrollable) {
			searchRequest.scroll(TimeValue.timeValueMinutes(Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class)));
		}
	}
	
	
	
	
	
	
}
