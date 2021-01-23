package org.takeshi.jdbc.esqlj.parser.model;

import java.util.ArrayList;
import java.util.List;

public class Index {
	String name;

	String alias;
	
	List<JoinedIndex> joinedIndex = new ArrayList<JoinedIndex>();
	
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

	public List<JoinedIndex> getJoinedIndex() {
		return joinedIndex;
	}

	public void setJoinedIndex(List<JoinedIndex> joinedIndex) {
		this.joinedIndex = joinedIndex;
	}

	public Condition getCondition() {
		return condition;
	}

	public void setCondition(Condition condition) {
		this.condition = condition;
	}
	
	
 
}
