package org.takeshi.jdbc.esqlj;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class LiveConnectionTestUnit
{
	private static Connection connection;
	
	@BeforeClass
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
		DatabaseMetaData metadata = connection.getMetaData();
		ResultSet rs = metadata.getCatalogs();
		assertNotNull(rs);
		assertFalse(rs.next());
		assertEquals(rs.getString(0), "");
	}
    
    @Test
	public void checkSchemas() throws SQLException {
		DatabaseMetaData metadata = connection.getMetaData();
		ResultSet rs = metadata.getSchemas();
		assertNotNull(rs);
		assertFalse(rs.next());
		assertTrue(rs.getString(0).length() > 0);
	}
    
    @Test
	public void checkTablesMetaData() throws SQLException {
		DatabaseMetaData metadata = connection.getMetaData();
		ResultSet rs = metadata.getTables(null, null, null, null);
		assertNotNull(rs);
		assertTrue(rs.getMetaData().getColumnCount() == 10);
		assertEquals(rs.getMetaData().getColumnLabel(0), "TABLE_CAT");
		assertTrue(rs.getMetaData().getColumnType(0) == java.sql.Types.VARCHAR);
	}
    
    @Test
	public void checkViews() throws SQLException {
		DatabaseMetaData metadata = connection.getMetaData();
		ResultSet rs = metadata.getTables(null, null, "VIEW", null);
		assertNotNull(rs);
		assertTrue(rs.getMetaData().getColumnCount() == 10);
		assertEquals(rs.getMetaData().getColumnLabel(0), "TABLE_CAT");
		assertTrue(rs.getMetaData().getColumnType(0) == java.sql.Types.VARCHAR);
	}
    
}
