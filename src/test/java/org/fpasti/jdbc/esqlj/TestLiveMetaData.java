package org.fpasti.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.fpasti.jdbc.esqlj.testUtils.ElasticLiveEnvironment;
import org.fpasti.jdbc.esqlj.testUtils.ElasticLiveUnit;
import org.fpasti.jdbc.esqlj.testUtils.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

@ElasticLiveUnit
@ExtendWith(ElasticLiveEnvironment.class)
public class TestLiveMetaData
{

	@Test
	public void simpleSelect() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex"));
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals("keywordField", rsm.getColumnName(1));
		assertEquals("keywordField", rsm.getColumnLabel(1));
	}
	
	@Test
	public void selectStart() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT * from testIndex"));
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals("booleanField", rsm.getColumnName(1));
		assertEquals("doubleField", rsm.getColumnName(2));
		assertEquals("geoPointField", rsm.getColumnName(3));
		assertEquals("integerField", rsm.getColumnName(4));
		assertEquals("keywordField", rsm.getColumnName(5));
		assertEquals("longField", rsm.getColumnName(6));
		assertEquals("object.keywordObjectField", rsm.getColumnName(7));
		assertEquals("textField", rsm.getColumnName(8));
		assertEquals("timestampField", rsm.getColumnName(9));
		assertEquals("_id", rsm.getColumnName(10));
		assertEquals("_score", rsm.getColumnName(11));
		assertEquals(11, rsm.getColumnCount());
	}

	@Test
	public void simpleSelectWithAlias() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField keywordFieldAlias from testIndex"));
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals("keywordField", rsm.getColumnName(1));
		assertEquals("keywordFieldAlias", rsm.getColumnLabel(1));
	}
	
	@Test
	public void simpleSelectWithAliasWithAs() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField AS keywordFieldAlias from testIndex"));
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals("keywordField", rsm.getColumnName(1));
		assertEquals("keywordFieldAlias", rsm.getColumnLabel(1));
	}

	@Test
	public void selectWithDoubleQuotedColumn() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT \"keywordField\" from testIndex"));
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(rsm.getColumnName(1), "keywordField");
		assertEquals(rsm.getColumnLabel(1), "keywordField");
	}
	
	@Test
	public void selectWithDoubleQuotedAlias() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField AS \"keywordFieldAlias\" from testIndex"));
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(rsm.getColumnName(1), "keywordField");
		assertEquals(rsm.getColumnLabel(1), "keywordFieldAlias");
	}
}
