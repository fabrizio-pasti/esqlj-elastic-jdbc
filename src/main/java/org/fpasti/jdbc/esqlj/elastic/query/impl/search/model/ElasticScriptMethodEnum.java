package org.fpasti.jdbc.esqlj.elastic.query.impl.search.model;

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