package org.takeshi.jdbc.esqlj.elastic.query.statement;

import net.sf.jsqlparser.statement.Statement;

public class SqlStatementInsert extends SqlStatement {

	public SqlStatementInsert(Statement statement) {
		super(SqlStatementType.SELECT);
	}

}
