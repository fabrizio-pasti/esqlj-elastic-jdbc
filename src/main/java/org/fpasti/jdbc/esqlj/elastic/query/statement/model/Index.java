package org.fpasti.jdbc.esqlj.elastic.query.statement.model;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

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
