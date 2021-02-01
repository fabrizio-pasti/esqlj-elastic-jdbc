package org.takeshi.jdbc.esqlj.elastic.query.statement;

import net.sf.jsqlparser.statement.Statement;

public class SqlStatementDelete extends SqlStatement {

	public SqlStatementDelete(Statement statement) {
		super(SqlStatementType.SELECT);
	}

}
