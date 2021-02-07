package org.fpasti.jdbc.esqlj.elastic.query.statement;

import net.sf.jsqlparser.statement.Statement;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class SqlStatementInsert extends SqlStatement {

	public SqlStatementInsert(Statement statement) {
		super(SqlStatementType.SELECT);
	}

}
