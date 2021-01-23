package org.takeshi.jdbc.esqlj.parser.model;

import java.util.ArrayList;
import java.util.List;

public class ParsedQuery {
	
	List<Field> fields= new ArrayList<Field>();
    
	Index index = new Index();

	public List<Field> getFields() {
		return fields;
	}

	public void setFields(List<Field> fields) {
		this.fields = fields;
	}

	public Index getIndex() {
		return index;
	}

	public void setIndex(Index index) {
		this.index = index;
	}
	

	
	
}
