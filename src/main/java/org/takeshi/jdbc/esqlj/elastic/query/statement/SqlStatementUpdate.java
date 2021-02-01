package org.takeshi.jdbc.esqlj.elastic.query.statement;

import net.sf.jsqlparser.statement.Statement;

public class SqlStatementUpdate extends SqlStatement {

	public SqlStatementUpdate(Statement statement) {
		super(SqlStatementType.SELECT);
	}

}
