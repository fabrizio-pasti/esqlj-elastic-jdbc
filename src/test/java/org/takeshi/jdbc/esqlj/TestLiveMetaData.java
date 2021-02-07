package org.takeshi.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

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
public class TestLiveMetaData
{

	@Test
	public void simpleSelect() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex"));
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(rsm.getColumnName(1), "keywordField");
		assertEquals(rsm.getColumnLabel(1), "keywordField");
	}
	
	@Test
	public void selectStart() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT * from testIndex"));
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(rsm.getColumnName(1), "booleanField");
		assertEquals(rsm.getColumnName(2), "doubleField");
		assertEquals(rsm.getColumnName(3), "geoPointField");
		assertEquals(rsm.getColumnName(4), "integerField");
		assertEquals(rsm.getColumnName(5), "keywordField");
		assertEquals(rsm.getColumnName(6), "longField");
		assertEquals(rsm.getColumnName(7), "object.keywordObjectField");
		assertEquals(rsm.getColumnName(8), "textField");
		assertEquals(rsm.getColumnName(9), "timestampField");
		assertEquals(rsm.getColumnName(10), "_id");
		assertEquals(rsm.getColumnCount(), 10);
	}

	@Test
	public void simpleSelectWithAlias() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField keywordFieldAlias from testIndex"));
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(rsm.getColumnName(1), "keywordField");
		assertEquals(rsm.getColumnLabel(1), "keywordFieldAlias");
	}
	
	@Test
	public void simpleSelectWithAliasWithAs() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField AS keywordFieldAlias from testIndex"));
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(rsm.getColumnName(1), "keywordField");
		assertEquals(rsm.getColumnLabel(1), "keywordFieldAlias");
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
