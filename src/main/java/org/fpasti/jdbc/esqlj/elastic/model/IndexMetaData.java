package org.fpasti.jdbc.esqlj.elastic.model;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class IndexMetaData {
	private String index;
	private Map<String, ElasticObject> elasticObjects;
	private List<String> columnsName;
 
	public IndexMetaData(String index, Map<String, ElasticObject> fields) {
		super();
		this.index = index;
		this.elasticObjects = fields;
		resolveColumnNames();
	}

	public String getIndex() {
		return index;
	}

	public Map<String, ElasticObject> getElasticObjects() {
		return elasticObjects;
	}
	
	public List<String> getColumnsName() {
		return columnsName;
	}
	
	private void resolveColumnNames() {
		columnsName = elasticObjects.keySet().stream().sorted().collect(Collectors.toList());
	}

}
