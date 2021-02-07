package org.fpasti.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;

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
public class TestLiveQuerySelect
{
	@Test
	public void selectStar() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT * from testIndex"));
		rs.next();
		assertEquals(rs.getRow(), 1);
		assertNotNull(rs.getObject(1));
		assertNotNull(rs.getObject(2));
		assertNotNull(rs.getObject(3));
		assertNotNull(rs.getObject(4));
		assertNotNull(rs.getObject(5));
		assertNotNull(rs.getObject(6));
		assertNotNull(rs.getObject(7));
		assertNotNull(rs.getObject(8));
		assertNotNull(rs.getObject(9));
		assertNotNull(rs.getObject(10));
	}
	
	@Test
	public void selectWithStarIndex() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT * from testIndexStar"));
		rs.next();
		assertEquals(rs.getRow(), 1);
		assertNotNull(rs.getObject(1));
		assertNotNull(rs.getObject(2));
		assertNotNull(rs.getObject(3));
		assertNotNull(rs.getObject(4));
		assertNotNull(rs.getObject(5));
		assertNotNull(rs.getObject(6));
		assertNotNull(rs.getObject(7));
		assertNotNull(rs.getObject(8));
		assertNotNull(rs.getObject(9));
		assertNotNull(rs.getObject(10));
	}
	
	@Test
	public void selectColumns() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT timestampField, keywordField from testIndex"));
		rs.next();
		assertNotNull(rs.getObject(1));
		assertNotNull(rs.getObject(2));
		assertThrows(IndexOutOfBoundsException.class,() -> rs.getObject(3));
		assertEquals(rs.getObject(1).getClass(), LocalDateTime.class);
		assertEquals(rs.getObject(2).getClass(), String.class);
	}
	
	@Test
	public void selectColumnDoubleQuoted() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT \"keywordField\" from testIndex WHERE keywordField='keyword01'"));
		assertEquals(rs.next(), true);
		assertEquals(rs.getString(1), "keyword01");
		assertEquals(rs.getRow(), 1);
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectColumnWithDoubleQuotedAlias() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT timestampField, keywordField AS \"keywordFieldAlias\" from testIndex WHERE keywordField='keyword01'"));
		assertEquals(rs.next(), true);
		assertEquals(rs.getString(2), "keyword01");
		assertEquals(rs.getRow(), 1);
		rs.close();
		stmt.close();
	}
}
