package org.fpasti.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
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
public class TestLiveQueryWhereEsqlj
{
	@Test
	public void selectWhereEsqlj001() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id, _score FROM testIndex WHERE query_string('01|02', 'textField')"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "doc_01", "doc_02"));
		}
		assertEquals(2, rs.getRow());		
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhereEsqlj002() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id, _score FROM testIndex WHERE geo_bounding_box('geoPointField', 50, 8, 40.1, 10.2)"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "doc_01", "doc_02", "doc_03", "doc_04"));
		}
		assertEquals(4, rs.getRow());		
		rs.close();
		stmt.close();
	}
}
