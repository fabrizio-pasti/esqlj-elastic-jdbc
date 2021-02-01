package org.takeshi.jdbc.esqlj.elastic.query.statement;

import org.takeshi.jdbc.esqlj.elastic.query.statement.model.Index;

public class SqlStatement {
	private SqlStatementType type;
	protected Index index;
	
	public SqlStatement(SqlStatementType type) {
		super();
		this.type = type;
	}

	public SqlStatementType getType() {
		return type;
	}

	public Index getIndex() {
		return index;
	}

}
