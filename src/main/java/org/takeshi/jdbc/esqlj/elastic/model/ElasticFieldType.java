package org.takeshi.jdbc.esqlj.elastic.model;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Arrays;

public enum ElasticFieldType {

		BOOLEAN("boolean", "BOOL", Boolean.class, Types.BOOLEAN, 1, null, null, false, false, true, false),
		BYTE("short", "NUMBER", Byte.class, Types.TINYINT, 3, null, null, false, false, true, false),
		CONSTANT_KEYWORD("constant_keyword", "VARCHAR", String.class, Types.VARCHAR, 10922, "'", "'", true, false, true, false),
		DATE("date", "TIMESTAMP", LocalDateTime.class, Types.TIMESTAMP, 19, "'", "'", true, false, true, false),
		DATE_NANOS("date_nanos", "TIMESTAMP", LocalDateTime.class, Types.TIMESTAMP, 19, "'", "'", true, false, true, false),
		DOUBLE("double", "NUMBER", Double.class, Types.DOUBLE, 76, null, null, false, false, true, false),
		FLOAT("float", "NUMBER", Float.class, Types.FLOAT, 38, null, null, false, false, true, false),
		GEO_POINT("geo_point", "STRUCT", GeoPoint.class, Types.STRUCT, 0, "'", "'", false, false, true, true),
		HALF_FLOAT("half_float", "NUMBER", Float.class, Types.FLOAT, 16, null, null, false, false, true, false),
		INTEGER("integer", "NUMBER", Integer.class, Types.INTEGER, 10, null, null, false, false, true, false),
		IP("ip", "VARCHAR", String.class, Types.VARCHAR, 39, "'", "'", true, false, true, false),
		KEYWORD("keyword", "VARCHAR", String.class, Types.VARCHAR, 10922, "'", "'", true, false, true, false),
		LONG("long", "BIGINT", Long.class, Types.BIGINT, 19, null, null, false, false, true, false),
		OBJECT("object", "STRUCT", Object.class, Types.STRUCT, 0, "'", "'", false, false, true, true),
		SCALED_FLOAT("scaled_float", "NUMBER", Float.class, Types.FLOAT, 16, null, null, false, false, true, false),
		SHORT("short", "NUMBER", Short.class, Types.SMALLINT, 5, null, null, false, false, true, false),
		TEXT("text", "VARCHAR", String.class, Types.VARCHAR, 0, "'", "'", true, false, true, false),
		UNSIGNED_LONG("unsigned_long", "NUMBER", Long.class, Types.BIGINT, 19, null, null, false, true, true, false),
		WILDCARD("wildcard", "VARCHAR", String.class, Types.VARCHAR, 10922, "'", "'", true, false, true, false),
		UNKNOWN("UNKNOWN", null, null, 0, 0, null, null, false, false, false, false);
		
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
		

		ElasticFieldType(String elType, String sqlType, Class<?> clazz, int sqlTypeCode, int precision, String literalPrefix, String literalSuffix, boolean caseSensitive, boolean unsigned, boolean concrete, boolean udt) {
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