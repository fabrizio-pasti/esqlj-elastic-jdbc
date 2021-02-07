package org.takeshi.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.takeshi.jdbc.esqlj.testUtils.ElasticLiveEnvironment;
import org.takeshi.jdbc.esqlj.testUtils.ElasticLiveUnit;
import org.takeshi.jdbc.esqlj.testUtils.TestUtils;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

@ElasticLiveUnit
@ExtendWith(ElasticLiveEnvironment.class)
public class TestLiveResultSetMetadata
{

	private static Statement stmt;
	private static ResultSet rs;
	private static ResultSetMetaData rsm;
	
	@BeforeAll
	public static void init() throws SQLException {
		stmt = TestUtils.getLiveConnection().createStatement();
		rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT * from testIndex"));
		rsm = rs.getMetaData();
	}
	
	@AfterAll
	public static void cleanAll() throws SQLException {
		rs.close();
		stmt.close();
	}
	
	@ParameterizedTest(name = "Check column {1}")
	@CsvSource({
		"1, booleanField, 16, BOOL",
		"2, doubleField, 8, NUMBER",
		"3, geoPointField, 2002, STRUCT",
		"4, integerField, 4, NUMBER",
		"5, keywordField, 12, VARCHAR",
		"6, longField, -5, BIGINT",
		"7, object.keywordObjectField, 12, VARCHAR",
		"8, textField, 12, VARCHAR",
		"9, timestampField, 93, TIMESTAMP",
		"10, _id, 12, VARCHAR"
		})
	public void resultSetMetaData(int column, String name, int type, String typeName) throws SQLException {		
		assertEquals(rsm.getColumnName(column), name);
		assertEquals(rsm.getColumnType(column), type);
		assertEquals(rsm.getColumnTypeName(column), typeName);
	}
}
