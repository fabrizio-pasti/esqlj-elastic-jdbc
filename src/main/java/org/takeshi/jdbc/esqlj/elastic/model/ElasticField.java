package org.takeshi.jdbc.esqlj.elastic.model;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ElasticField implements Comparable<ElasticField> {
		public static String DOC_ID_ALIAS = "_id";
		private String fullName;
		private String name;
		private ElasticFieldType type;
		private Long size;

		public ElasticField(String fullName, ElasticFieldType type) {
			super();
			this.fullName = fullName;
			this.name = fullName.substring(fullName.lastIndexOf('.') + 1);
			this.type = type;
		}
		
		public ElasticField(String fullName, ElasticFieldType type, Long size) {
			this(fullName, type);
			this.size = size;
		}

		public String getFullName() {
			return fullName;
		}
		
		public String getName() {
			return name;
		}

		public ElasticFieldType getType() {
			return type;
		}

		public Long getSize() {
			return size;
		}
		
		@Override
		public int compareTo(ElasticField o) {
			return this.fullName.compareTo(o.getFullName());
		}
		
		public boolean isDocField() {
			return !type.equals(ElasticFieldType.TEXT);
		}
	}