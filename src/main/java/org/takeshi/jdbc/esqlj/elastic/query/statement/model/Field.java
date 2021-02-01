package org.takeshi.jdbc.esqlj.elastic.query.statement.model;

public class Field {
	String name;
	String alias;
	String index;
	
	public Field(String name, String alias, String index) {
		this.name = name.replace("\"", "");
		this.alias = alias;
		this.index = index;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

}
