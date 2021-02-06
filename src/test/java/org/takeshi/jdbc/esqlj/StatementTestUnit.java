package org.takeshi.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class StatementTestUnit
{
	private static Connection connection;
	
	/*@BeforeAll
    public static void init() throws SQLException {
		connection = TestUtils.getLiveConnection(null);
    } 
	
    @Test
	public void isPresent() throws SQLException {
		DatabaseMetaData metadata = connection.getMetaData();
		assertNotNull(metadata);
	}
    
    @Test
	public void checkCatalogs() throws SQLException {
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery("");
		ResultSetMetaData rsm = rs.getMetaData();
		for(int i = 1; i <= rsm.getColumnCount(); i++) {
			System.out.println(rsm.getColumnLabel(i) + " - " + rsm.getColumnTypeName(i) + " = ");
		}
		int idx = 0;
		while(rs.next()) {
			idx += 1;
			System.out.println("--------------------------------------------------- " + idx);
			for(int i = 1; i <= rsm.getColumnCount(); i++) {
				System.out.println(rsm.getColumnLabel(i) + " - " + rsm.getColumnTypeName(i) + " = " + rs.getObject(i));
			}
			System.out.println("");
		}
		
	}*/
    
}
