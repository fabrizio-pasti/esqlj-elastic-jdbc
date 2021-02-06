package org.takeshi.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.takeshi.jdbc.esqlj.testUtils.ElasticLiveEnvironment;
import org.takeshi.jdbc.esqlj.testUtils.ElasticLiveUnit;
import org.takeshi.jdbc.esqlj.testUtils.TestUtils;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

@ElasticLiveUnit
@ExtendWith(ElasticLiveEnvironment.class)
public class ConnectionLiveTestUnit
{
    
    @Test
	public void metadataExists() throws SQLException {
		DatabaseMetaData metadata = TestUtils.getLiveConnection().getMetaData();
		assertNotNull(metadata);
	}
     
    @Test
	public void catalogsExists() throws SQLException {
		DatabaseMetaData metadata = TestUtils.getLiveConnection().getMetaData();
		ResultSet rs = metadata.getCatalogs();
		assertNotNull(rs);
		rs.next();
		assertEquals(rs.getString(1), "");
		assertFalse(rs.next());
	}
    
    @Test
	public void schemasExists() throws SQLException {
		DatabaseMetaData metadata = TestUtils.getLiveConnection().getMetaData();
		ResultSet rs = metadata.getSchemas();
		assertNotNull(rs);
		rs.next();
		assertTrue(rs.getString(1).length() > 0);
		assertFalse(rs.next());
	}
    
    @Test
	public void tablesMetaDataExists() throws SQLException {
		DatabaseMetaData metadata = TestUtils.getLiveConnection().getMetaData();
		ResultSet rs = metadata.getTables(null, null, null, null);
		assertNotNull(rs);
		assertTrue(rs.getMetaData().getColumnCount() == 10);
		assertEquals(rs.getMetaData().getColumnLabel(3), "TABLE_NAME");
		assertTrue(rs.getMetaData().getColumnType(3) == java.sql.Types.VARCHAR);
	}
    
    @Test
	public void viewMetaDataExists() throws SQLException {
		DatabaseMetaData metadata = TestUtils.getLiveConnection()	.getMetaData();
		ResultSet rs = metadata.getTables(null, null, "VIEW", null);
		assertNotNull(rs);
		assertTrue(rs.getMetaData().getColumnCount() == 10);
		assertEquals(rs.getMetaData().getColumnLabel(3), "TABLE_NAME");
		assertTrue(rs.getMetaData().getColumnType(3) == java.sql.Types.VARCHAR);
	}
    
}
