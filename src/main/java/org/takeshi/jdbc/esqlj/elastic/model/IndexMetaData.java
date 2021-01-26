package org.takeshi.jdbc.esqlj.elastic.model;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.takeshi.jdbc.esqlj.parser.model.Field;

public class IndexMetaData {
	private String index;
	private Map<String, ElasticField> fields;
	private List<String> fieldsName;

	public IndexMetaData(String index, Map<String, ElasticField> fields) {
		super();
		this.index = index;
		this.fields = fields;
		resolveFieldNames();
	}

	public String getIndex() {
		return index;
	}

	public Map<String, ElasticField> getFields() {
		return fields;
	}
	
	public List<String> getFieldsName() {
		return fieldsName;
	}
	
	private void resolveFieldNames() {
		fieldsName = fields.keySet().stream().sorted().collect(Collectors.toList());
	}


}
