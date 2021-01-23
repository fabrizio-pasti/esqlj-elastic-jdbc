package org.takeshi.jdbc.esqlj.parser.model;

public class Condition {
	
	String condition;
	
	Field  leftField;
	
	Field rightField;

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public Field getLeftField() {
		return leftField;
	}

	public void setLeftField(Field leftField) {
		this.leftField = leftField;
	}

	public Field getRightField() {
		return rightField;
	}

	public void setRightField(Field rightField) {
		this.rightField = rightField;
	}
	
	

}
