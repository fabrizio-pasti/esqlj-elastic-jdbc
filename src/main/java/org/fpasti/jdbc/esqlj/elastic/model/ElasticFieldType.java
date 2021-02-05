package org.fpasti.jdbc.esqlj.elastic.model;

import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Arrays;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public enum ElasticFieldType {

		BOOLEAN("boolean", "BOOL", Boolean.class, Types.BOOLEAN, 1, null, null, false, false, true, false, false),
		BYTE("short", "NUMBER", Byte.class, Types.TINYINT, 3, null, null, false, false, true, false, false),
		CONSTANT_KEYWORD("constant_keyword", "VARCHAR", String.class, Types.VARCHAR, 10922, "'", "'", true, false, true, false, false),
		DATE("date", "TIMESTAMP", LocalDateTime.class, Types.TIMESTAMP, 19, "'", "'", true, false, true, false, false),
		DATE_NANOS("date_nanos", "TIMESTAMP", LocalDateTime.class, Types.TIMESTAMP, 19, "'", "'", true, false, true, false, false),
		DOC_ID("doc_id", "VARCHAR", String.class, Types.VARCHAR, 512, "'", "'", true, false, true, false, true),
		DOUBLE("double", "NUMBER", Double.class, Types.DOUBLE, 76, null, null, false, false, true, false, false),
		FLOAT("float", "NUMBER", Float.class, Types.FLOAT, 38, null, null, false, false, true, false, false),
		GEO_POINT("geo_point", "STRUCT", EsGeoPoint.class, Types.STRUCT, 0, "'", "'", false, false, true, true, false),
		HALF_FLOAT("half_float", "NUMBER", Float.class, Types.FLOAT, 16, null, null, false, false, true, false, false),
		INTEGER("integer", "NUMBER", Integer.class, Types.INTEGER, 10, null, null, false, false, true, false, false),
		IP("ip", "VARCHAR", String.class, Types.VARCHAR, 39, "'", "'", true, false, true, false, false),
		KEYWORD("keyword", "VARCHAR", String.class, Types.VARCHAR, 10922, "'", "'", true, false, true, false, false),
		LONG("long", "BIGINT", Long.class, Types.BIGINT, 19, null, null, false, false, true, false, false),
		OBJECT("object", "STRUCT", Object.class, Types.STRUCT, 0, "'", "'", false, false, true, true, false),
		SCALED_FLOAT("scaled_float", "NUMBER", Float.class, Types.FLOAT, 16, null, null, false, false, true, false, false),
		SHORT("short", "NUMBER", Short.class, Types.SMALLINT, 5, null, null, false, false, true, false, false),
		TEXT("text", "VARCHAR", String.class, Types.VARCHAR, 0, "'", "'", true, false, true, false, false),
		UNSIGNED_LONG("unsigned_long", "NUMBER", Long.class, Types.BIGINT, 19, null, null, false, true, true, false, false),
		WILDCARD("wildcard", "VARCHAR", String.class, Types.VARCHAR, 10922, "'", "'", true, false, true, false, false),
		UNKNOWN("UNKNOWN", null, null, 0, 0, null, null, false, false, false, false, false);
		
		String elType;
		String sqlType;
		Class<?> clazz;
		int sqlTypeCode;
		int precision;
		String literalPrefix;
		String literalSuffix;
		boolean caseSensitive;
		boolean unsigned;
		boolean concrete;	
		boolean udt;
		boolean primaryKey;
		

		ElasticFieldType(String elType, String sqlType, Class<?> clazz, int sqlTypeCode, int precision, String literalPrefix, String literalSuffix, boolean caseSensitive, boolean unsigned, boolean concrete, boolean udt, boolean primaryKey) {
			this.elType = elType;
			this.sqlType = sqlType;
			this.clazz = clazz;
			this.sqlTypeCode = sqlTypeCode;
			this.precision = precision;
			this.literalPrefix = literalPrefix;
			this.literalSuffix = literalSuffix;
			this.caseSensitive = caseSensitive;
			this.unsigned = unsigned;
			this.concrete = concrete;
			this.udt = udt;
			this.primaryKey = primaryKey;
		}
		
		public String getElasticType() {
			return elType;
		}
		
		public String getSqlType() {
			return sqlType;
		}
		
		@SuppressWarnings("rawtypes")
		public Class getClazz() {
			return clazz;
		}

		public int getSqlTypeCode() {
			return sqlTypeCode;
		}

		public int getPrecision() {
			return precision;
		}

		public String getLiteralPrefix() {
			return literalPrefix;
		}

		public String getLiteralSuffix() {
			return literalSuffix;
		}

		public boolean isCaseSensitive() {
			return caseSensitive;
		}

		public boolean isUnsigned() {
			return unsigned;
		}
		
		public boolean isConcrete() {
			return concrete;
		}

		public boolean isUdt() {
			return udt;
		}
		
		public boolean isPrimaryKey() {
			return primaryKey;
		}


		public static ElasticFieldType resolveByElasticType(String elType) {
			return Arrays.stream(ElasticFieldType.values()).filter(elt -> elt.elType.equals(elType)).findFirst()
					.orElseGet(null);
		}

		public static ElasticFieldType resolveBySqlType(String sqlType) {
			return Arrays.stream(ElasticFieldType.values()).filter(elt -> elt.sqlType.equals(sqlType)).findFirst()
					.orElseGet(null);
		}

		public static ElasticFieldType resolveByValue(Object value) {
			return Arrays.stream(ElasticFieldType.values()).filter(elt -> value.getClass().equals(elt.getClazz())).findFirst()
					.orElseGet(null);
		}
		
	}