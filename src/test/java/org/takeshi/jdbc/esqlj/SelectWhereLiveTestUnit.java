package org.takeshi.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;

import org.apache.commons.lang3.StringUtils;
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
public class SelectWhereLiveTestUnit
{	
	@Test
	public void selectWhere001() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE keywordField='keyword01'"));
		assertEquals(rs.next(), true);
		assertEquals(rs.getString(1), "keyword01");
		assertEquals(rs.next(), false);
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere002() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE keywordField='keyword01...'"));
		assertEquals(rs.next(), false);
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere003() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE NOT keywordField='keyword01'"));
		while(rs.next()) {
			assertNotEquals(rs.getString(1), "keyword01");
		}
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere004() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE keywordField='keyword01' or keywordField='keyword02' or keywordField='keyword03'"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "keyword01", "keyword02", "keyword03"));
		}
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere005() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE NOT keywordField='keyword01' or keywordField='keyword02' or keywordField='keyword03'"));
		while(rs.next()) {
			assertFalse(rs.getString(1).equals("keyword01"));
		}
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere006() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE NOT (keywordField='keyword01' or keywordField='keyword02' or keywordField='keyword03')"));
		while(rs.next()) {
			assertFalse(rs.getString(1).equals("keyword01"));
		}
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere007() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE integerField=1 AND NOT (keywordField='keyword01' or keywordField='keyword02' or keywordField='keyword03')"));
		assertFalse(rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere008() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE integerField=1 AND keywordField='keyword01' OR keywordField='keyword02'"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "keyword01", "keyword02"));
		}
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere009() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE integerField=1 AND keywordField='keyword02' OR keywordField='keyword02'"));
		while(rs.next()) {
			assertEquals(rs.getString(1), "keyword02");
		}
		rs.close();
		stmt.close();
	}
}
