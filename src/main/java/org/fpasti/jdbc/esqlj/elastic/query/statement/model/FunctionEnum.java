package org.fpasti.jdbc.esqlj.elastic.query.statement.model;

import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldType;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public enum FunctionEnum {
		AVG(true, ElasticFieldType.DOUBLE),
		COUNT(true, ElasticFieldType.LONG),
		LATITUDE(false, ElasticFieldType.DOUBLE),
		LONGITUDE(false, ElasticFieldType.DOUBLE),
		MIN(true, ElasticFieldType.DOUBLE),
		MAX(true, ElasticFieldType.DOUBLE),
		SUM(true, ElasticFieldType.DOUBLE),
		TO_CHAR(false, ElasticFieldType.TEXT);
		
		boolean aggregating;
		ElasticFieldType type;
		
		FunctionEnum(boolean aggregating, ElasticFieldType type) {
			this.aggregating = aggregating;
			this.type = type;
		}
		
		public boolean isAggregating() {
			return aggregating;
		}

		public ElasticFieldType getType() {
			return type;
		}

}