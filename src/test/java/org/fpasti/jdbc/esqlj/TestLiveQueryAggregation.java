package org.fpasti.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;
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

}
