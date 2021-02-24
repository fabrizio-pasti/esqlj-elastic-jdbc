package org.fpasti.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;

import org.fpasti.jdbc.esqlj.testUtils.ElasticLiveEnvironment;
import org.fpasti.jdbc.esqlj.testUtils.ElasticLiveUnit;
import org.fpasti.jdbc.esqlj.testUtils.ElasticTestService;
import org.fpasti.jdbc.esqlj.testUtils.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

@ElasticLiveUnit
@ExtendWith(ElasticLiveEnvironment.class)
public class TestLiveQueryAggregation
{
	@Test
	public void selectAggregation001() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT COUNT(*) from testIndex"));
		assertEquals(true, rs.next());
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getLong(1));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}

	@Test
	public void selectAggregation002() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT COUNT(*) from testIndex WHERE keywordField='keyword01'"));
		assertEquals(true, rs.next());
		assertEquals(1L, rs.getLong(1));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	
	@Test
	public void selectAggregation003() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT COUNT(keywordField) from testIndex"));
		assertEquals(true, rs.next());
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getLong(1));
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectAggregation004() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT COUNT(\"object.keywordObjectField\") from testIndex"));
		assertEquals(true, rs.next());
		assertEquals(ElasticTestService.getNumberOfDocs() - 1, rs.getLong(1));
		rs.close();
		stmt.close();
	}

	@Test
	public void selectAggregation005() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT COUNT(keywordField), COUNT(\"object.keywordObjectField\") from testIndex"));
		assertEquals(true, rs.next());
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getLong(1));
		assertEquals(ElasticTestService.getNumberOfDocs() - 1, rs.getLong(2));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}

	@Test
	public void selectAggregation006() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT COUNT(*), COUNT(\"object.keywordObjectField\") from testIndex"));
		assertEquals(true, rs.next());
		assertEquals(7L, rs.getLong(1));
		assertEquals(6L, rs.getLong(2));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}

	@Test
	public void selectAggregation007() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT booleanField, AVG(integerField), COUNT(*) from testIndex GROUP BY booleanField"));
		assertEquals(true, rs.next());
		assertEquals(false, rs.getBoolean(1));
		assertEquals(4, rs.getLong(3));
		assertEquals(4.5, rs.getDouble(2), 0.001);
		assertEquals(true, rs.next());
		assertEquals(true, rs.getBoolean(1));
		assertEquals(ElasticTestService.getNumberOfDocs() - 4, rs.getLong(3));
		assertEquals(3, rs.getDouble(2), 0.001);
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectAggregation008() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		SQLSyntaxErrorException e = assertThrows(SQLSyntaxErrorException.class, () -> {
			stmt.executeQuery(TestUtils.resolveTestIndex("SELECT COUNT(*), booleanField from testIndex"));
		});
		assertEquals("Cannot be mixed Expression and Column in SELECT clause without GROUP BY aggregations", e.getMessage());
		stmt.close();
	}
	
	@Test
	public void selectAggregation009() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT booleanField, AVG(integerField) test, SUM(longField), COUNT(*) from testIndex GROUP BY booleanField ORDER BY test DESC"));
		assertEquals(true, rs.next());
		assertEquals(false, rs.getBoolean(1));
		assertEquals(4.5, rs.getDouble(2), 0.001);
		assertEquals(18.0, rs.getDouble(3), 0.001);
		assertEquals(4L, rs.getLong(4));
		assertEquals(true, rs.next());
		assertEquals(true, rs.getBoolean(1));
		assertEquals(3.0, rs.getDouble(2), 0.001);
		assertEquals(9, rs.getDouble(3), 0.001);
		assertEquals(3L, rs.getLong(4));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectAggregation010() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT booleanField, AVG(integerField) test, SUM(longField), COUNT(*) from testIndex GROUP BY booleanField HAVING AVG(integerField)>=4 OR SUM(longField)>=19"));
		assertEquals(true, rs.next());
		assertEquals(false, rs.getBoolean(1));
		assertEquals(4.5, rs.getDouble(2), 0.001);
		assertEquals(18.0, rs.getDouble(3), 0.001);
		assertEquals(4L, rs.getLong(4));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectAggregation011() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT booleanField, AVG(integerField) test, SUM(longField), COUNT(*) from testIndex GROUP BY booleanField HAVING test>=4"));
		assertEquals(true, rs.next());
		assertEquals(false, rs.getBoolean(1));
		assertEquals(4.5, rs.getDouble(2), 0.001);
		assertEquals(18.0, rs.getDouble(3), 0.001);
		assertEquals(4L, rs.getLong(4));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectAggregation012() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT booleanField, AVG(integerField) test, SUM(longField), COUNT(*) from testIndex GROUP BY booleanField HAVING test>=5"));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectAggregation013() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT booleanField, AVG(integerField) test, SUM(longField), COUNT(*) from testIndex GROUP BY booleanField HAVING COUNT(*)>3"));
		assertEquals(true, rs.next());
		assertEquals(false, rs.getBoolean(1));
		assertEquals(4.5, rs.getDouble(2), 0.001);
		assertEquals(18.0, rs.getDouble(3), 0.001);
		assertEquals(4L, rs.getLong(4));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectAggregation014() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT SUM(longField), AVG(longField), COUNT(*) from testIndex"));
		assertEquals(true, rs.next());
		assertEquals(27.0, rs.getDouble(1), 0.001);
		assertEquals(3.857143, rs.getDouble(2), 0.001);
		assertEquals(7L, rs.getLong(3));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectAggregation015() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT COUNT(\"object.keywordObjectField\") from testIndex"));
		assertEquals(true, rs.next());
		assertEquals(6L, rs.getLong(1));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectAggregation016() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT COUNT(DISTINCT \"object.keywordObjectField\") from testIndex"));
		assertEquals(true, rs.next());
		assertEquals(5L, rs.getLong(1));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
}
