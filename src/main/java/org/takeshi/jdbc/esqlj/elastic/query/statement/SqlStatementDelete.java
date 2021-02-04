package org.takeshi.jdbc.esqlj.elastic.query.statement;

import net.sf.jsqlparser.statement.Statement;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class SqlStatementDelete extends SqlStatement {

	public SqlStatementDelete(Statement statement) {
		super(SqlStatementType.SELECT);
	}

}
