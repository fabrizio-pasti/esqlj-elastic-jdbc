package org.fpasti.jdbc.esqlj.elastic.metadata;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetFieldMappingsResponse;
import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticObject;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.fpasti.jdbc.esqlj.elastic.model.IndexMetaData;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class MetaDataService {
	private ElasticServerDetails elasticServerDetails;
	private RestHighLevelClient client;
	private Map<String, IndexMetaData> cacheIndexMetaData = new HashMap<String, IndexMetaData>();
	
	public MetaDataService(RestHighLevelClient client) throws SQLException {
		this.client = client;
		retrieveElasticInfo();
	}

	private void retrieveElasticInfo() throws SQLException {
		try {
			MainResponse response = client.info(RequestOptions.DEFAULT);
			
			setElasticServerDetails(new ElasticServerDetails(response));
			
		} catch (IOException e) {
			throw new SQLException("Failed to retrieve info from Elastic");
		}
	}

	public String getProductName() {
		return "Elasticsearch";
	}
	
	public ElasticServerDetails getElasticServerDetails() {
		return elasticServerDetails;
	}

	public void setElasticServerDetails(ElasticServerDetails elasticServerDetails) {
		this.elasticServerDetails = elasticServerDetails;
	}

	public IndexMetaData getIndexMetaData(String index) throws SQLException {
		if(Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_INDEX_METADATA_CACHE, Boolean.class)) {
			synchronized(cacheIndexMetaData) {
				if(!cacheIndexMetaData.containsKey(index)) {
					cacheIndexMetaData.put(index, new IndexMetaData(index, getIndexFields(index)));
				}
			}
			
			return cacheIndexMetaData.get(index);
		}

		return new IndexMetaData(index, getIndexFields(index));
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, ElasticObject> getIndexFields(String index) throws SQLException {
		try {
			GetFieldMappingsRequest request = new GetFieldMappingsRequest();
			request.indices(index);
			request.fields("*");
			
			GetFieldMappingsResponse response = client.indices().getFieldMapping(request, RequestOptions.DEFAULT);
	
			Map<String, ElasticObject> fields = new TreeMap<String, ElasticObject>();
			List<String> managedFields = new ArrayList<String>();
			
			response.mappings().entrySet().stream().map(entry -> entry.getValue()).forEach(iMap -> {
				iMap.forEach((field, metadata) -> {
					Map<String, Object> metadataMap = metadata.sourceAsMap();
					if(metadataMap.size() > 0 && !managedFields.stream().anyMatch(field::equals)) {
						Map<String, Object> fieldMap = (Map<String, Object>)metadataMap.get(field.substring(field.lastIndexOf('.') + 1));

						ElasticFieldType fieldType = ElasticFieldType.resolveByElasticType((String)fieldMap.get("type"));
						fields.put(field, new ElasticObject(
								field, 
								fieldType,
								fieldMap.get("ignore_above") != null ? new Long((Integer)fieldMap.get("ignore_above")) : null,
								fieldMap.get("doc_values") != null ? (boolean)fieldMap.get("doc_values") : isDocValue(fieldType)));
						managedFields.add(field);
					}			
				});
			});
			
			return fields.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,(oldValue, newValue) -> oldValue, LinkedHashMap::new));
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
	}

	private boolean isDocValue(ElasticFieldType fieldType) {
		switch(fieldType) {
			case TEXT:
				return false;
			case BINARY:
				return false;
			default:
				return true;
		}
	}
	
	
}
