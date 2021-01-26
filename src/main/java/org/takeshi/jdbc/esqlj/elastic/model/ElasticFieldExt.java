package org.takeshi.jdbc.esqlj.elastic.model;

public class ElasticFieldExt extends ElasticField {

	private String columnName;

	public ElasticFieldExt(String fullName, ElasticFieldType type, Long size) {
		super(fullName, type, size);
	}

	public static ElasticFieldExt promoteInstance(ElasticField elasticField, String alias) {
		ElasticFieldExt ext = new ElasticFieldExt(elasticField.getFullName(), elasticField.getType(),
				elasticField.getSize());
		ext.setColumnName(alias != null ? alias : elasticField.getFullName());
		return ext;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
}