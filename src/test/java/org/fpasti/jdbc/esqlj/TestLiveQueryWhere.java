package org.fpasti.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
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
public class TestLiveQueryWhere
{
	@Test
	public void selectWhere001() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE keywordField='keyword01'"));
		assertEquals(true, rs.next());
		assertEquals("keyword01", rs.getString(1));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere002() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE keywordField='keyword01...'"));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere003() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE NOT keywordField='keyword01'"));
		while(rs.next()) {
			assertNotEquals("keyword01", rs.getString(1));
		}
		assertEquals(ElasticTestService.getNumberOfDocs() - 1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere004() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE keywordField='keyword01' OR keywordField='keyword02' OR keywordField='keyword03'"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "keyword01", "keyword02", "keyword03"));
		}
		assertEquals(3, rs.getRow());
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
		assertEquals(ElasticTestService.getNumberOfDocs() - 1, rs.getRow());
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
		assertEquals(ElasticTestService.getNumberOfDocs() - 3, rs.getRow());
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
		assertEquals(2, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere009() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE integerField=1 AND keywordField='keyword02' OR keywordField='keyword02'"));
		while(rs.next()) {
			assertEquals("keyword02", rs.getString(1));
		}
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere010() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE integerField=1 OR (integerField>1 AND doubleField<3.0)"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "keyword01", "keyword02"));
		}
		assertEquals(2, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere011() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE timestampField>TO_DATE('2020/06/01', 'yyyy/mm/dd')"));
		while(rs.next()) {
			assertFalse(rs.getString(1).equals("keyword01"));
		}
		assertEquals(ElasticTestService.getNumberOfDocs() - 1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere012() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE keywordField='keyword01' AND timestampField>TO_DATE('2020/06/01', 'yyyy/mm/dd')"));
		assertEquals(false, rs.next());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere013() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE keywordField='keyword02' OR (integerField > 2 AND longField <= 3) AND timestampField>TO_DATE('2020/06/01', 'yyyy/mm/dd')"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "keyword02", "keyword03"));
		}
		assertEquals(2, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere014() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE keywordField='keyword02' AND (integerField >= 1 AND longField <= 3) AND timestampField>TO_DATE('2020/06/01', 'yyyy/mm/dd')"));
		while(rs.next()) {
			assertTrue(rs.getString(1).equals("keyword02"));
		}
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere015() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE keywordField='keyword02' OR NOT keywordField='keyword03' AND integerField>4 AND integerField<=5"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "keyword02", "keyword05"));
		}
		assertEquals(2, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere016() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE timestampField<SYSDATE"));
		while(rs.next()) {}
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere017() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE timestampField<SYSDATE()"));
		while(rs.next()) {}
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere018() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE timestampField<NOW()"));
		while(rs.next()) {}
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere019() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE timestampField<GETDATE()"));
		while(rs.next()) {}
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getRow());
		rs.close();
		stmt.close();
	}

	@Test
	public void selectWhere020() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE timestampField<TRUNC(SYSDATE)"));
		while(rs.next()) {}
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere021() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE timestampField<TRUNC(SYSDATE())"));
		while(rs.next()) {}
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere022() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT keywordField from testIndex WHERE timestampField<TRUNC(NOW())"));
		while(rs.next()) {}
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere023() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT \"keywordField\" from testIndex WHERE keywordField='keyword01'"));
		assertEquals(true, rs.next());
		assertEquals("keyword01", rs.getString(1));
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere024() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT \"keywordField\" from testIndex WHERE keywordField LIKE 'keyword*'"));
		while(rs.next()) {}
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere025() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT \"keywordField\" from testIndex WHERE keywordField NOT LIKE 'keyword*'"));
		while(rs.next()) {}
		assertEquals(ElasticTestService.getNumberOfDocs(), rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere026() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT \"keywordField\" from testIndex WHERE textField LIKE '*04'"));
		while(rs.next()) {
			assertEquals("keyword04", rs.getString(1));
		}
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere027() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE textField LIKE '*04'"));
		while(rs.next()) {
			assertEquals("doc_04", rs.getString(1));
		}
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere028() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE textField LIKE '*04'"));
		while(rs.next()) {
			assertEquals("doc_04", rs.getString(1));
		}
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere029() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE _id != 'doc_04'"));
		while(rs.next()) {
			assertNotEquals("doc_04", rs.getString(1));
		}
		assertEquals(ElasticTestService.getNumberOfDocs() - 1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere030() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE _id='doc_04' OR _id!='doc_01'"));
		while(rs.next()) {
			assertNotEquals("doc_01", rs.getString(1));
		}
		assertEquals(ElasticTestService.getNumberOfDocs() - 1, rs.getRow());
		rs.close();
		stmt.close();
	}

	@Test
	public void selectWhere031() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE _id='doc_04' OR _id!='doc_01' AND _id!='doc_02'"));
		while(rs.next()) {
			assertNotEquals(StringUtils.containsAny(rs.getString(1), "doc_01", "doc_02"), rs.getString(1));
		}
		assertEquals(ElasticTestService.getNumberOfDocs() - 2, rs.getRow());
		rs.close();
		stmt.close();
	}

	@Test
	public void selectWhere032() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE \"object.keywordObjectField\" IS NULL"));
		while(rs.next()) {
			assertEquals("doc_06", rs.getString(1));
		}
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere033() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE \"object.keywordObjectField\" IS NOT NULL"));
		while(rs.next()) {
			assertNotEquals("doc_06", rs.getString(1));
		}
		assertEquals(ElasticTestService.getNumberOfDocs() - 1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere034() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE EXTRACT(YEAR FROM timestampField)=2020"));
		while(rs.next()) {
			assertEquals("doc_01", rs.getString(1));
		}
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere035() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE EXTRACT(MONTH FROM timestampField)=5"));
		while(rs.next()) {
			assertEquals("doc_01", rs.getString(1));
		}
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere036() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE EXTRACT(DAY FROM timestampField)=25"));
		while(rs.next()) {
			assertEquals("doc_01", rs.getString(1));
		}
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere037() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE EXTRACT(HOUR FROM timestampField)=20 OR EXTRACT(HOUR FROM timestampField)=5"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "doc_01", "doc_06"));
		}
		assertEquals(2, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere038() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE EXTRACT(MINUTE FROM timestampField)=10"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "doc_01", "doc_05", "doc_06"));
		}
		assertEquals(3, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere039() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE EXTRACT(SECOND FROM timestampField)=20"));
		while(rs.next()) {
			assertTrue(StringUtils.containsAny(rs.getString(1), "doc_01", "doc_02", "doc_03", "doc_04", "doc_05", "doc_06"));
		}
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere040() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex WHERE timestampField BETWEEN TO_DATE('2020/01/01', 'yyyy/mm/dd') AND TO_DATE('2020/12/31', 'yyyy/mm/dd')"));
		while(rs.next()) {
			assertEquals("doc_01", rs.getString(1));
		}
		assertEquals(1, rs.getRow());
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere041() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex  WHERE integerField<=6 ORDER BY keywordField DESC"));
		rs.next();
		assertEquals("doc_06", rs.getString(1));
		rs.next();
		assertEquals("doc_05", rs.getString(1));
		rs.next();
		assertEquals("doc_04", rs.getString(1));
		rs.next();
		assertEquals("doc_03", rs.getString(1));
		rs.next();
		assertEquals("doc_02", rs.getString(1));
		rs.next();
		assertEquals("doc_01", rs.getString(1));
		rs.close();
		stmt.close();
	}
	
	@Test
	public void selectWhere042() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT _id from testIndex LIMIT 2"));
		while(rs.next()) {}
		assertEquals(2, rs.getRow());
		stmt.close();
	}
	
	@Test
	public void selectWhere043() throws SQLException {
		Statement stmt = TestUtils.getLiveConnection().createStatement();
		ResultSet rs = stmt.executeQuery(TestUtils.resolveTestIndex("SELECT * from testIndex WHERE booleanField=true"));
		while(rs.next()) {}
		assertEquals(3, rs.getRow());
		stmt.close();
	}
}
