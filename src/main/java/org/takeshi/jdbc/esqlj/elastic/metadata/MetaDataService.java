package org.takeshi.jdbc.esqlj.elastic.metadata;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetFieldMappingsResponse;
import org.takeshi.jdbc.esqlj.Configuration;
import org.takeshi.jdbc.esqlj.ConfigurationEnum;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticField;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.model.IndexMetaData;

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
		if(Configuration.getConfiguration(ConfigurationEnum.CFG_INDEX_METADATA_CACHE, Boolean.class)) {
			if(!cacheIndexMetaData.containsKey(index)) {
				cacheIndexMetaData.put(index, new IndexMetaData(index, getIndexFields(index)));
			}
			
			return cacheIndexMetaData.get(index);
		}

		return new IndexMetaData(index, getIndexFields(index));
	}
	
	@SuppressWarnings("unchecked")
	public List<ElasticField> getIndexFields(String index) throws SQLException {
		try {
			GetFieldMappingsRequest request = new GetFieldMappingsRequest();
			request.indices(index);
			request.fields("*");
			
			GetFieldMappingsResponse response = client.indices().getFieldMapping(request, RequestOptions.DEFAULT);
	
			List<ElasticField> fields = new ArrayList<ElasticField>();
			List<String> managedFields = new ArrayList<String>();
			
			response.mappings().entrySet().stream().map(entry -> entry.getValue()).forEach(iMap -> {
				iMap.forEach((field, metadata) -> {
					Map<String, Object> metadataMap = metadata.sourceAsMap();
					if(metadataMap.size() > 0 && !managedFields.stream().anyMatch(field::equals)) {
						fields.add(new ElasticField(field, ElasticFieldType.resolveByElasticType(((Map<String, String>)metadataMap.get(field.substring(field.lastIndexOf('.') + 1))).get("type"))));
						managedFields.add(field);
					}			
				});
			});
			
			return fields.stream().sorted().collect(Collectors.toList());
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	
}
