package org.takeshi.jdbc.esqlj.elastic.metadata;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticField;

public class MetaDataService {
	private ElasticServerDetails elasticServerDetails;
	
	public MetaDataService(RestHighLevelClient client) throws SQLException {
		retrieveElasticInfo(client);
	}

	private void retrieveElasticInfo(RestHighLevelClient client) throws SQLException {
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

	public List<ElasticField> getIndexFields(String index) {
		return null;
	}
}
