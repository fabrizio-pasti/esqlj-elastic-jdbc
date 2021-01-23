package org.takeshi.jdbc.esqlj.elastic.model;

public class ElasticField implements Comparable<ElasticField> {
		private String name;
		private ElasticFieldType type;

		public ElasticField(String name, ElasticFieldType type) {
			super();
			this.name = name;
			this.type = type;
		}

		public String getName() {
			return name;
		}

		public ElasticFieldType getType() {
			return type;
		}

		@Override
		public int compareTo(ElasticField o) {
			return this.name.compareTo(o.getName());
		}

	}