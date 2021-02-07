package org.fpasti.jdbc.esqlj.elastic.query.statement;

import net.sf.jsqlparser.statement.Statement;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class SqlStatementUpdate extends SqlStatement {

	public SqlStatementUpdate(Statement statement) {
		super(SqlStatementType.SELECT);
	}

}
