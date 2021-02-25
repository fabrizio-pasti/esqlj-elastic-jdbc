package org.fpasti.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.ResultSet;
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
public class TestLiveQueryGeoPoint
{
	@Test
	public void selectGeoPoint001() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT LATITUDE(geoPointField), LONGITUDE(geoPointField), keywordField from testIndex ORDER BY keywordField LIMIT 1" ));
		assertEquals(true, rs.next());
		assertEquals(45.43623246252537, rs.getDouble(1), 0.001);
		assertEquals(9.240137469023466, rs.getDouble(2), 0.001);
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
}
