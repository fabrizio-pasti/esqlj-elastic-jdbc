package org.fpasti.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
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
public class TestLiveQueryDistinct
{
	@Test
	public void selectDistinct001() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT DISTINCT keywordField, booleanField from testIndex ORDER BY keywordField, booleanField DESC"));
		assertEquals(true, rs.next());
		assertEquals("keyword01", rs.getString(1));
		assertEquals(true, rs.getBoolean(2));
		assertEquals(true, rs.next());
		assertEquals("keyword02", rs.getString(1));
		assertEquals(false, rs.getBoolean(2));
		assertEquals(true, rs.next());
		assertEquals("keyword03", rs.getString(1));
		assertEquals(true, rs.getBoolean(2));
		assertEquals(true, rs.next());
		assertEquals("keyword04", rs.getString(1));
		assertEquals(false, rs.getBoolean(2));
		assertEquals(true, rs.next());
		assertEquals("keyword05", rs.getString(1));
		assertEquals(true, rs.getBoolean(2));
		assertEquals(true, rs.next());
		assertEquals("keyword06", rs.getString(1));
		assertEquals(false, rs.getBoolean(2));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectDistinct002() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		SQLSyntaxErrorException e = assertThrows(SQLSyntaxErrorException.class, () -> {
			stmt.executeQuery(TestUtils.resolveTestIndex("SELECT DISTINCT COUNT(keywordField) from testIndex"));
		});
		assertEquals("DISTINCT clause shall apply only to columns, not expressions", e.getMessage());
		stmt.close();
	}
}
