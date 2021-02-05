package org.fpasti.jdbc.esqlj.elastic.query.statement;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.Index;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

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
