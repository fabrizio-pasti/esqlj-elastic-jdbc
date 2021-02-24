package org.fpasti.jdbc.esqlj.elastic.query.statement.model;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public enum QueryType {
	DOCS,
	DISTINCT_DOCS,
	AGGR_COUNT_ALL,
	AGGR_UNGROUPED_EXPRESSIONS,
	AGGR_GROUP_BY
}
