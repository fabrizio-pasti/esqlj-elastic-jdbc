package org.takeshi.jdbc.esqlj.elastic.query;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.elastic.query.impl.ElasticQuery;
import org.takeshi.jdbc.esqlj.elastic.query.statement.SqlStatement;
import org.takeshi.jdbc.esqlj.elastic.query.statement.SqlStatementDelete;
import org.takeshi.jdbc.esqlj.elastic.query.statement.SqlStatementInsert;
import org.takeshi.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.takeshi.jdbc.esqlj.elastic.query.statement.SqlStatementType;
import org.takeshi.jdbc.esqlj.elastic.query.statement.SqlStatementUpdate;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class Executor {
	
	public static ElasticQuery execSql(EsConnection connection, String sql) throws SQLException {
		try {
			return new ElasticQuery(connection, (SqlStatementSelect)parseQuery(sql, SqlStatementType.SELECT));
		} catch(SQLException se) {
			throw se;
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	private static SqlStatement parseQuery(String sql, SqlStatementType requiredType) throws SQLSyntaxErrorException {
		try {
			Statement statement =  CCJSqlParserUtil.parse(sql);
			switch(statement.getClass().getSimpleName()) {
				case "Select":
					if(requiredType != null && requiredType != SqlStatementType.SELECT) {
						throw new SQLSyntaxErrorException("Not an SELECT statement");
					}
					return new SqlStatementSelect(statement);
				case "Update":
					if(requiredType != null && requiredType != SqlStatementType.UPDATE) {
						throw new SQLSyntaxErrorException("Not an UPDATE statement");
					}
					return new SqlStatementUpdate(statement);
				case "Insert":
					if(requiredType != null && requiredType != SqlStatementType.INSERT) {
						throw new SQLSyntaxErrorException("Not an INSERT statement");
					}
					return new SqlStatementInsert(statement);
				case "Delete":
					if(requiredType != null && requiredType != SqlStatementType.DELETE) {
						throw new SQLSyntaxErrorException("Not an DELETE statement");
					}
					return new SqlStatementDelete(statement);
				default:
					throw new SQLSyntaxErrorException("Unrecognized statement");
			}
		} catch(Exception e) {
			throw new SQLSyntaxErrorException(e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
		}
	}
	
}
