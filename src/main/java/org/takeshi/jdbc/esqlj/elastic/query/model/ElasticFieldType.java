package org.takeshi.jdbc.esqlj.elastic.query.model;

import java.sql.Types;
import java.util.Arrays;

public enum ElasticFieldType {
		
		BOOLEAN("boolean", "BOOLEAN", Types.BOOLEAN),
		DATA("date", "DATE", Types.DATE),
		DATA_TIME("datetime", "TIMESTAMP", Types.TIMESTAMP),
		DOUBLE("double", "DOUBLE", Types.DOUBLE),
		GEO_POINT("geo_point", "STRUCT", Types.STRUCT),
		INTEGER("integer", "INTEGER", Types.INTEGER),
		KEYWORD("keyword", "VARCHAR", Types.VARCHAR),
		LONG("long", "BIGINT", Types.BIGINT),
		OBJECT("object", "STRUCT", Types.STRUCT),
		TEXT("text", "VARCHAR", Types.VARCHAR);
		
		String elType;
		String sqlType;
		int sqlTypeCode;

		ElasticFieldType(String elType, String sqlType, int sqlTypeCode) {
			this.elType = elType;
			this.sqlType = sqlType;
			this.sqlTypeCode = sqlTypeCode;
		}
		
		public String getElasticType() {
			return elType;
		}
		
		public String getSqlType() {
			return sqlType;
		}

		public int getSqlTypeCode() {
			return sqlTypeCode;
		}

		public static ElasticFieldType parseElasticType(String elType) {
			return Arrays.asList(ElasticFieldType.values()).stream().filter(elt -> elt.elType.equals(elType)).findFirst()
					.orElseGet(null);
		}

		public static ElasticFieldType parseSqlType(String sqlType) {
			return Arrays.asList(ElasticFieldType.values()).stream().filter(elt -> elt.sqlType.equals(sqlType)).findFirst()
					.orElseGet(null);
		}
		
	}