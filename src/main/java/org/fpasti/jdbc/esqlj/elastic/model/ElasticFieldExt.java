package org.fpasti.jdbc.esqlj.elastic.model;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ElasticFieldExt extends ElasticField {

	private String columnName;
	private QueryColumn declaredQueryColumn;

	public ElasticFieldExt(String fullName, ElasticFieldType type) {
		super(fullName, type);
		setColumnName(fullName);
	}

	public ElasticFieldExt(QueryColumn queryColumn) {
		super(queryColumn.getAlias(), queryColumn.getAggregatingType());
		setColumnName(queryColumn.getAlias());
		setDeclaredQueryColumn(queryColumn);
	}

	public ElasticFieldExt(String fullName, ElasticFieldType type, Long size, boolean docValue) {
		super(fullName, type, size, docValue);
	}

	public static ElasticFieldExt promoteInstance(ElasticField elasticField, QueryColumn queryColumn) {
		ElasticFieldExt ext = new ElasticFieldExt(elasticField.getFullName(), elasticField.getType(),
				elasticField.getSize(), elasticField.isDocValue());
		ext.setColumnName(queryColumn.getAlias() != null ? queryColumn.getAlias() : elasticField.getFullName());
		ext.setDeclaredQueryColumn(queryColumn);
		return ext;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public QueryColumn getDeclaredQueryColumn() {
		return declaredQueryColumn;
	}

	public void setDeclaredQueryColumn(QueryColumn declaredQueryColumn) {
		this.declaredQueryColumn = declaredQueryColumn;
	}
	
	
}