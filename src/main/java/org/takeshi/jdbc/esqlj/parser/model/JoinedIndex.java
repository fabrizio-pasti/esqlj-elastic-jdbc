package org.takeshi.jdbc.esqlj.parser.model;

public class JoinedIndex {
	
	String name; 
	
	String alias;
	
	Condition condition = new Condition();

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

	public Condition getCondition() {
		return condition;
	}

	public void setCondition(Condition condition) {
		this.condition = condition;
	}
	
	

}
