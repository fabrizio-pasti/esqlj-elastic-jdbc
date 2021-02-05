package org.fpasti.jdbc.esqlj.elastic.query.impl;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticObjectType;
import org.fpasti.jdbc.esqlj.elastic.query.AbstractOneShotQuery;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class IndicesQuery extends AbstractOneShotQuery {
			
	private static String[] COLUMNS =  {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"};
	
	public IndicesQuery(EsConnection connection, ElasticObjectType... types) throws SQLException {
		super(connection, "system_indices", COLUMNS);
		
		if(Arrays.asList(types).contains(ElasticObjectType.INDEX)) {
			init(ElasticObjectType.INDEX);
		}
		
		if(Arrays.asList(types).contains(ElasticObjectType.ALIAS)) {
			init(ElasticObjectType.ALIAS);
		}
	}

	public void init(ElasticObjectType type) throws SQLException {
		List<String> indices = type == ElasticObjectType.INDEX ? retrieveIndices() : retrieveAliases();
		indices.forEach(indice -> {
			Map<String, Object> data = new HashMap<String, Object>();
			data.put("TABLE_NAME", indice);
			data.put("TABLE_TYPE", type == ElasticObjectType.INDEX ? "TABLE" : "VIEW");
			data.put("REMARKS", "");
			insertRowWithData(data);
		});
	}
	
	private List<String> retrieveIndices() throws SQLException {
		try {
			GetIndexRequest indexRequest = new GetIndexRequest("*");
			GetIndexResponse indexResponse = getConnection().getElasticClient().indices().get(indexRequest, RequestOptions.DEFAULT);
			List<String> indices = new ArrayList<String>(Arrays.asList(indexResponse.getIndices()));
			return indices.stream().sorted().collect(Collectors.toList());
		} catch(IOException e) {
			throw new SQLException("Failed to retrieve indices and aliases from Elastic");
		}
	}

	private List<String> retrieveAliases() throws SQLException {
		try {
			GetIndexRequest indexRequest = new GetIndexRequest("*");
			GetIndexResponse indexResponse = getConnection().getElasticClient().indices().get(indexRequest, RequestOptions.DEFAULT);
			List<String> aliases = new ArrayList<String>(indexResponse.getAliases().values().stream().flatMap(val ->  val.stream().map(alias -> alias.alias())).collect(Collectors.toSet()));
			return aliases.stream().sorted().collect(Collectors.toList());
		} catch(IOException e) {
			throw new SQLException("Failed to retrieve indices and aliases from Elastic");
		}
	}
}
