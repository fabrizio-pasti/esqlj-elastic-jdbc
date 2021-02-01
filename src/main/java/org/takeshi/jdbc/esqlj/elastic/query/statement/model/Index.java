package org.takeshi.jdbc.esqlj.elastic.query.statement.model;

public class Index {
	String name;

	String alias;

	public Index(String name, String alias) {
		this.name = name;
		this.alias = alias;
	}

	public String getName() {
		return name;
	}

	public String getAlias() {
		return alias;
	}
 
}
