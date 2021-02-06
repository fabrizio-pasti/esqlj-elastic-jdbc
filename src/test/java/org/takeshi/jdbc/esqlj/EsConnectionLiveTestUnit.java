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
import org.takeshi.jdbc.esqlj.test.ElasticLiveEnvironment;
import org.takeshi.jdbc.esqlj.test.ElasticLiveUnit;
import org.takeshi.jdbc.esqlj.test.TestUtils;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

@ElasticLiveUnit
@ExtendWith(ElasticLiveEnvironment.class)
public class EsConnectionLiveTestUnit
{
    
    @Test
	public void isPresent() throws SQLException {
		DatabaseMetaData metadata = TestUtils.getLiveConnection().getMetaData();
		assertNotNull(metadata);
	}
     
    @Test
	public void checkCatalogs() throws SQLException {
		DatabaseMetaData metadata = TestUtils.getLiveConnection().getMetaData();
		ResultSet rs = metadata.getCatalogs();
		assertNotNull(rs);
		rs.next();
		assertEquals(rs.getString(1), "");
		assertFalse(rs.next());
	}
    
    @Test
	public void checkSchemas() throws SQLException {
		DatabaseMetaData metadata = TestUtils.getLiveConnection().getMetaData();
		ResultSet rs = metadata.getSchemas();
		assertNotNull(rs);
		rs.next();
		assertTrue(rs.getString(1).length() > 0);
		assertFalse(rs.next());
	}
    
    @Test
	public void checkTablesMetaData() throws SQLException {
		DatabaseMetaData metadata = TestUtils.getLiveConnection().getMetaData();
		ResultSet rs = metadata.getTables(null, null, null, null);
		assertNotNull(rs);
		assertTrue(rs.getMetaData().getColumnCount() == 10);
		assertEquals(rs.getMetaData().getColumnLabel(3), "TABLE_NAME");
		assertTrue(rs.getMetaData().getColumnType(3) == java.sql.Types.VARCHAR);
	}
    
    @Test
	public void checkViews() throws SQLException {
		DatabaseMetaData metadata = TestUtils.getLiveConnection()	.getMetaData();
		ResultSet rs = metadata.getTables(null, null, "VIEW", null);
		assertNotNull(rs);
		assertTrue(rs.getMetaData().getColumnCount() == 10);
		assertEquals(rs.getMetaData().getColumnLabel(3), "TABLE_NAME");
		assertTrue(rs.getMetaData().getColumnType(3) == java.sql.Types.VARCHAR);
	}
    
}
