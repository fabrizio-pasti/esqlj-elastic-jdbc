package org.fpasti.jdbc.esqlj.elastic.query.impl.search.model;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public enum ElasticScriptMethodEnum {
	YEAR("getYear()"),
	MONTH("getMonthValue()"),
	DAY("getDayOfMonth()"),
	HOUR("getHour()"),
	MINUTE("getMinute()"),
	SECOND("getSecond()");
	
	String method;
	
	ElasticScriptMethodEnum(String method) {
		this.method = method;
	}
	
	public String getMethod() {
		return method;
	}
	
}