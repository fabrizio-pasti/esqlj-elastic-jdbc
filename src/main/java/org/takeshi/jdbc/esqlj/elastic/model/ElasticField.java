package org.takeshi.jdbc.esqlj.elastic.model;

public class ElasticField implements Comparable<ElasticField> {
		private String fullName;
		private String name;
		private ElasticFieldType type;

		public ElasticField(String fullName, ElasticFieldType type) {
			super();
			this.fullName = fullName;
			this.name = fullName.substring(fullName.lastIndexOf('.') + 1);
			this.type = type;
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

		@Override
		public int compareTo(ElasticField o) {
			return this.fullName.compareTo(o.getFullName());
		}

	}