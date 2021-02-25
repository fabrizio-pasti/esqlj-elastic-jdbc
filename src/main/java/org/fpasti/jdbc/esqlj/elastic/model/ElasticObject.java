package org.fpasti.jdbc.esqlj.elastic.model;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ElasticObject implements Comparable<ElasticObject> {
		public static String DOC_ID_ALIAS = "_id";
		public static String DOC_SCORE = "_score";
		private String fullName;
		private String name;
		private ElasticFieldType type;
		private Long size;
		private boolean docValue;
		private QueryColumn linkedQueryColumn;
		
		public ElasticObject() {
		}
		
		public ElasticObject(String fullName, ElasticFieldType type) {
			this.fullName = fullName;
			this.name = fullName.substring(fullName.lastIndexOf('.') + 1);
			this.type = type;
		}
		
		public ElasticObject(String fullName, ElasticFieldType type, Long size, boolean docValue) {
			this(fullName, type);
			this.size = size;
			this.docValue = docValue;
		}
		
		public ElasticObject(QueryColumn queryColumn) {
			this(queryColumn.getAlias(), queryColumn.getAggregatingType());
			this.linkedQueryColumn = queryColumn;
		}

		public String getFullName() {
			return fullName;
		}
		
		public String getName() {
			return name;
		}
		
		public String getColumnName() {
			return linkedQueryColumn != null && linkedQueryColumn.getAlias() != null ? linkedQueryColumn.getAlias() : getFullName();
		}

		public ElasticFieldType getType() {
			return type;
		}

		public Long getSize() {
			return size;
		}
		
		@Override
		public int compareTo(ElasticObject o) {
			return this.fullName.compareTo(o.getFullName());
		}
		
		public boolean isSourceField() {
			return type.equals(ElasticFieldType.TEXT);
		}

		public boolean isDocValue() {
			return docValue;
		}

		public QueryColumn getLinkedQueryColumn() {
			return linkedQueryColumn;
		}

		public void setLinkedQueryColumn(QueryColumn linkedQueryColumn) {
			this.linkedQueryColumn = linkedQueryColumn;
		}
		
		public void setFullName(String fullName) {
			this.fullName = fullName;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setType(ElasticFieldType type) {
			this.type = type;
		}

		public void setSize(Long size) {
			this.size = size;
		}

		public void setDocValue(boolean docValue) {
			this.docValue = docValue;
		}

		public ElasticObject clone() {
			ElasticObject cloned = new ElasticObject();
			cloned.setDocValue(docValue);
			cloned.setFullName(fullName);
			cloned.setLinkedQueryColumn(linkedQueryColumn);
			cloned.setName(name);
			cloned.setSize(size);
			cloned.setType(type);
			return cloned;
		}

	}