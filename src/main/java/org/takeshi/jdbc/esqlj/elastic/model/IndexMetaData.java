package org.takeshi.jdbc.esqlj.elastic.model;

import java.util.List;
import java.util.stream.Collectors;

public class IndexMetaData {
	private String index;
	private List<ElasticField> fields;
	private List<String> fieldsName;

	public IndexMetaData(String index, List<ElasticField> fields) {
		super();
		this.index = index;
		this.fields = fields;
		resolveFieldNames();
	}

	public String getIndex() {
		return index;
	}

	public List<ElasticField> getFields() {
		return fields;
	}
	
	public List<String> getFieldsName() {
		return fieldsName;
	}
	
	private void resolveFieldNames() {
		fieldsName = fields.stream().map(field -> field.getFullName()).collect(Collectors.toList());
	}

}
