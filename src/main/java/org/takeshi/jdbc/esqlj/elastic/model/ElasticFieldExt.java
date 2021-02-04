package org.takeshi.jdbc.esqlj.elastic.model;

import org.takeshi.jdbc.esqlj.elastic.query.statement.model.Field;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ElasticFieldExt extends ElasticField {

	private String columnName;
	private Field selectField;

	public ElasticFieldExt(String fullName, ElasticFieldType type, Long size) {
		super(fullName, type, size);
	}

	public static ElasticFieldExt promoteInstance(ElasticField elasticField, Field selectField) {
		ElasticFieldExt ext = new ElasticFieldExt(elasticField.getFullName(), elasticField.getType(),
				elasticField.getSize());
		ext.setColumnName(selectField.getAlias() != null ? selectField.getAlias() : elasticField.getFullName());
		ext.setSelectField(selectField);
		return ext;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public Field getSelectField() {
		return selectField;
	}

	public void setSelectField(Field selectField) {
		this.selectField = selectField;
	}


	
}