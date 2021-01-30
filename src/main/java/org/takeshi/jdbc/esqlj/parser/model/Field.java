package org.takeshi.jdbc.esqlj.parser.model;

public class Field {
	String name;
	String alias;
	String index;
	String value;
	
	public Field(String name, String alias, String index) {		
		this.name = name;
		this.alias = alias;
		this.index = index;
	}
	
	public Field(String name, String alias, String index, String value) {		
		this.name = name;
		this.alias = alias;
		this.index = index;
		this.value = value;
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

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	
}
