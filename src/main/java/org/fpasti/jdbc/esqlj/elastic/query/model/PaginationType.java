package org.fpasti.jdbc.esqlj.elastic.query.model;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public enum PaginationType {
	NO_SCROLL,
	SCROLL_API,
	BY_ORDER,
	BY_ORDER_WITH_PIT
}
