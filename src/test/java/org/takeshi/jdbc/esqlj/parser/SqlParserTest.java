package org.takeshi.jdbc.esqlj.parser;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.takeshi.jdbc.esqlj.parser.model.ECondition;
import org.takeshi.jdbc.esqlj.parser.model.Field;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

import junitparams.JUnitParamsRunner;
import net.sf.jsqlparser.JSQLParserException;

@RunWith(JUnitParamsRunner.class)
public class SqlParserTest {

	
	@Test
	public void parseSimpleQuery () throws SQLException, JSQLParserException {
		// given
		String sql1 = "SELECT   *    FROM TABLE1";		
		
		// when
		ParsedQuery qry1 = SqlParser.parse(sql1);
		
		//then
		assertTrue(qry1.getFields().size()==1);
		assertTrue(qry1.getFields().get(0).getName().equals("*"));	
		assertNull(qry1.getFields().get(0).getIndex());
		assertNull(qry1.getFields().get(0).getAlias());	
		assertTrue(qry1.getIndex().getName().equals("TABLE1"));
		assertTrue(qry1.getIndex().getAlias().equals("TABLE1"));		
		
	}
	
	@Test
	public void parseQuery () throws SQLException, JSQLParserException {
		// given		
		String sql = "SELECT *    FROM TABLE1 T1 WHERE T1.COL1=5 AND COL2>=10 OR COL2 < 12";
		
		// when
		ParsedQuery qry = SqlParser.parse(sql);
		
		//then
		assertTrue(qry.getFields().size()==1);
		assertTrue(qry.getFields().get(0).getName().equals("*"));	
		assertNull(qry.getFields().get(0).getIndex());
		assertNull(qry.getFields().get(0).getAlias());	
		assertTrue(qry.getIndex().getName().equals("TABLE1"));
		assertTrue(qry.getIndex().getAlias().equals("T1"));		
		assertTrue(qry.getIndex().getJoinedIndex().size()==0);
		
		assertTrue(qry.getIndex().getCondition().get(0).getCondition().compareTo(ECondition.EqualsTo)==0);
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getName().equals("COL1"));
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getIndex().equals("T1"));
		assertNull(qry.getIndex().getCondition().get(0).getLeftField().getAlias());
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getValue().equals("5"));
		
		assertTrue(qry.getIndex().getCondition().get(1).getCondition().compareTo(ECondition.GreaterThanEquals)==0);
		assertTrue(qry.getIndex().getCondition().get(1).getLeftField().getName().equals("COL2"));
		assertNull(qry.getIndex().getCondition().get(1).getLeftField().getIndex());
		assertNull(qry.getIndex().getCondition().get(1).getLeftField().getAlias());
		assertTrue(qry.getIndex().getCondition().get(1).getLeftField().getValue().equals("10"));
		
		assertTrue(qry.getIndex().getCondition().get(2).getCondition().compareTo(ECondition.MinorThan)==0);
		assertTrue(qry.getIndex().getCondition().get(2).getLeftField().getName().equals("COL2"));
		assertNull(qry.getIndex().getCondition().get(1).getLeftField().getIndex());
		assertNull(qry.getIndex().getCondition().get(2).getLeftField().getAlias());
		assertTrue(qry.getIndex().getCondition().get(2).getLeftField().getValue().equals("12"));
		
	}
		
	@Test
	public void parseMultilineQuery () throws SQLException, JSQLParserException {
		// given		
		String sql = "SELECT T1.COL10 PrimaColonna,T1.COL11 AS  SecondaColonna \n" +
				"FROM TABLE1 AS T1 \n" +
				"WHERE T1.COL1 like '%VALORE%' \n" +
				"AND T1.COL2 != 150";
		
		// when
		ParsedQuery qry = SqlParser.parse(sql);
		
		//then
		assertTrue(qry.getFields().size()==2);
		assertTrue(qry.getFields().get(0).getName().equals("COL10"));	
		assertTrue(qry.getFields().get(0).getIndex().equals("T1"));
		assertTrue(qry.getFields().get(0).getAlias().equals("PrimaColonna"));	
		assertTrue(qry.getFields().get(1).getName().equals("COL11"));	
		assertTrue(qry.getFields().get(1).getIndex().equals("T1"));
		assertTrue(qry.getFields().get(1).getAlias().equals("SecondaColonna"));
		assertTrue(qry.getIndex().getName().equals("TABLE1"));
		assertTrue(qry.getIndex().getAlias().equals("T1"));		
		assertTrue(qry.getIndex().getJoinedIndex().size()==0);
		
		assertTrue(qry.getIndex().getCondition().get(0).getCondition().compareTo(ECondition.LikeExpression)==0);
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getName().equals("COL1"));
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getIndex().equals("T1"));
		assertNull(qry.getIndex().getCondition().get(0).getLeftField().getAlias());
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getValue().equals("'%VALORE%'"));
		
		assertTrue(qry.getIndex().getCondition().get(1).getCondition().compareTo(ECondition.NotEqualsTo)==0);
		assertTrue(qry.getIndex().getCondition().get(1).getLeftField().getName().equals("COL2"));
		assertTrue(qry.getIndex().getCondition().get(1).getLeftField().getIndex().equals("T1"));
		assertNull(qry.getIndex().getCondition().get(1).getLeftField().getAlias());
		assertTrue(qry.getIndex().getCondition().get(1).getLeftField().getValue().equals("150"));
		
	}
	
	@Test
	public void parseInConditionQuery () throws SQLException, JSQLParserException {
		// given		
		String sql = "SELECT T1.COL10 PrimaColonna,T1.COL11 AS  SecondaColonna \n" +
					 "FROM TABLE1 T1 \n " + 
					 "WHERE T1.COL1 in (5,10,15,20)";
		
		// when
		ParsedQuery qry = SqlParser.parse(sql);
		
		//then
		assertTrue(qry.getFields().size()==2);
		assertTrue(qry.getFields().get(0).getName().equals("COL10"));	
		assertTrue(qry.getFields().get(0).getIndex().equals("T1"));
		assertTrue(qry.getFields().get(0).getAlias().equals("PrimaColonna"));	
		assertTrue(qry.getFields().get(1).getName().equals("COL11"));	
		assertTrue(qry.getFields().get(1).getIndex().equals("T1"));
		assertTrue(qry.getFields().get(1).getAlias().equals("SecondaColonna"));
		assertTrue(qry.getIndex().getName().equals("TABLE1"));
		assertTrue(qry.getIndex().getAlias().equals("T1"));		
		assertTrue(qry.getIndex().getJoinedIndex().size()==0);
		
		assertTrue(qry.getIndex().getCondition().get(0).getCondition().compareTo(ECondition.InExpression)==0);
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getName().equals("COL1"));
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getIndex().equals("T1"));
		assertNull(qry.getIndex().getCondition().get(0).getLeftField().getAlias());
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getValue().equals("(5, 10, 15, 20)"));		
	}
	
	
	
	@Test
	public void parseTable () throws SQLException, JSQLParserException {
		
		// given
		Map<String,String> tables = new HashMap<String,String>();
		ParsedQuery qry1 = new ParsedQuery();
		ParsedQuery qry2 = new ParsedQuery();
		ParsedQuery qry3 = new ParsedQuery();
		String fieldValue1 = "TABLE1 AS T1";
		String fieldValue2 = "TABLE1 T1";
		String fieldValue3 = "TABLE1";
		
		// when
		SqlParser.parseTable(fieldValue1, qry1, tables);
		SqlParser.parseTable(fieldValue2, qry2, tables);
		SqlParser.parseTable(fieldValue3, qry3, tables);
		
		// then
		assertTrue(tables.size()==2);
		assertTrue(tables.containsKey("T1"));
		assertTrue(tables.containsKey("TABLE1"));
		assertTrue(tables.get("T1").equals("TABLE1"));
		assertTrue(tables.get("TABLE1").equals("TABLE1"));
		
		assertTrue(qry1.getIndex().getName().equals("TABLE1"));
		assertTrue(qry1.getIndex().getAlias().equals("T1"));
		assertTrue(qry2.getIndex().getName().equals("TABLE1"));
		assertTrue(qry2.getIndex().getAlias().equals("T1"));
		assertTrue(qry3.getIndex().getName().equals("TABLE1"));
		assertTrue(qry3.getIndex().getAlias().equals("TABLE1"));
		
	}
	
	@Test
	public void parseFiels () throws SQLException, JSQLParserException {
		// given
		String fieldValue1 = "TABLE_NAME.COL_NAME AS COL_ALIAS";
		String fieldValue2 = "TABLE_NAME.COL_NAME COL_ALIAS";
		String fieldValue3 = "TABLE_NAME.COL_NAME";
		String fieldValue4 = "COL_NAME";
		String fieldValue5 = "COL_NAME COL_ALIAS";
		String fieldValue6 = "COL_NAME AS COL_ALIAS";
		
		String fieldValue7 = "TABLE_NAME.\"FK_NAME.COL_NAME\" AS COL_ALIAS";
		String fieldValue8 = "TABLE_NAME.\"FK_NAME.COL_NAME\" COL_ALIAS";
		String fieldValue9 = "TABLE_NAME.\"FK_NAME.COL_NAME\"";
		String fieldValue10 = "\"FK_NAME.COL_NAME\"";
		String fieldValue11= "\"FK_NAME.COL_NAME\" COL_ALIAS";
		String fieldValue12 = "\"FK_NAME.COL_NAME\" AS COL_ALIAS";
		
		// when
		Field  field1 = SqlParser.parseField(fieldValue1);
		Field  field2 = SqlParser.parseField(fieldValue2);
		Field  field3 = SqlParser.parseField(fieldValue3);
		Field  field4 = SqlParser.parseField(fieldValue4);
		Field  field5 = SqlParser.parseField(fieldValue5);
		Field  field6 = SqlParser.parseField(fieldValue6);		
		Field  field7 = SqlParser.parseField(fieldValue7);
		Field  field8 = SqlParser.parseField(fieldValue8);
		Field  field9 = SqlParser.parseField(fieldValue9);
		Field  field10 = SqlParser.parseField(fieldValue10);
		Field  field11 = SqlParser.parseField(fieldValue11);
		Field  field12 = SqlParser.parseField(fieldValue12);		
		
		// then
		assertTrue(field1.getName().equals("COL_NAME"));
		assertTrue(field1.getIndex().equals("TABLE_NAME"));
		assertTrue(field1.getAlias().equals("COL_ALIAS"));
		assertTrue(field7.getName().equals("\"FK_NAME.COL_NAME\""));
		assertTrue(field7.getIndex().equals("TABLE_NAME"));
		assertTrue(field7.getAlias().equals("COL_ALIAS"));
		
		assertTrue(field2.getName().equals("COL_NAME"));
		assertTrue(field2.getIndex().equals("TABLE_NAME"));
		assertTrue(field2.getAlias().equals("COL_ALIAS"));
		assertTrue(field8.getName().equals("\"FK_NAME.COL_NAME\""));
		assertTrue(field8.getIndex().equals("TABLE_NAME"));
		assertTrue(field8.getAlias().equals("COL_ALIAS"));
		
		assertTrue(field3.getName().equals("COL_NAME"));
		assertTrue(field3.getIndex().equals("TABLE_NAME"));
		assertNull(field3.getAlias());		
		assertTrue(field9.getName().equals("\"FK_NAME.COL_NAME\""));
		assertTrue(field9.getIndex().equals("TABLE_NAME"));
		assertNull(field9.getAlias());
		
		assertTrue(field4.getName().equals("COL_NAME"));
		assertNull(field4.getIndex());
		assertNull(field4.getAlias());
		assertTrue(field10.getName().equals("\"FK_NAME.COL_NAME\""));
		assertNull(field10.getIndex());
		assertNull(field10.getAlias());
		
		assertTrue(field5.getName().equals("COL_NAME"));
		assertNull(field5.getIndex());
		assertTrue(field5.getAlias().equals("COL_ALIAS"));
		assertTrue(field11.getName().equals("\"FK_NAME.COL_NAME\""));
		assertNull(field11.getIndex());
		assertTrue(field11.getAlias().equals("COL_ALIAS"));
		
		assertTrue(field6.getName().equals("COL_NAME"));
		assertNull(field6.getIndex());
		assertTrue(field6.getAlias().equals("COL_ALIAS"));		
		assertTrue(field12.getName().equals("\"FK_NAME.COL_NAME\""));
		assertNull(field12.getIndex());
		assertTrue(field12.getAlias().equals("COL_ALIAS"));
		
	}
	
	@Test
	public void parseStarFiels () throws SQLException, JSQLParserException {
		// given
		String fieldValue1 = "TABLE_NAME.* AS COL_ALIAS";
		String fieldValue2 = "TABLE_NAME.* COL_ALIAS";
		String fieldValue3 = "TABLE_NAME.*";
		String fieldValue4 = "*";
		String fieldValue5 = "* COL_ALIAS";
		String fieldValue6 = "* AS COL_ALIAS";

		String fieldValue7 = "TABLE_NAME.\"FK_NAME.*\" AS COL_ALIAS";
		String fieldValue8 = "TABLE_NAME.\"FK_NAME.*\" COL_ALIAS";
		String fieldValue9 = "TABLE_NAME.\"FK_NAME.*\"";
		String fieldValue10 = "\"FK_NAME.*\"";
		String fieldValue11 = "\"FK_NAME.*\" COL_ALIAS";
		String fieldValue12 = "\"FK_NAME.*\" AS COL_ALIAS";

		// when	
		Field  field1 = SqlParser.parseField(fieldValue1);		
		Field  field2 = SqlParser.parseField(fieldValue2);
		Field  field3 = SqlParser.parseField(fieldValue3);
		Field  field4 = SqlParser.parseField(fieldValue4);
		Field  field5 = SqlParser.parseField(fieldValue5);
		Field  field6 = SqlParser.parseField(fieldValue6);
		Field  field7 = SqlParser.parseField(fieldValue7);
		Field  field8 = SqlParser.parseField(fieldValue8);
		Field  field9 = SqlParser.parseField(fieldValue9);
		Field  field10 = SqlParser.parseField(fieldValue10);
		Field  field11 = SqlParser.parseField(fieldValue11);
		Field  field12 = SqlParser.parseField(fieldValue12);

		// then
		assertTrue(field1.getName().equals("*"));
		assertTrue(field1.getIndex().equals("TABLE_NAME"));
		assertTrue(field1.getAlias().equals("COL_ALIAS"));
		assertTrue(field7.getName().equals("\"FK_NAME.*\""));
		assertTrue(field7.getIndex().equals("TABLE_NAME"));
		assertTrue(field7.getAlias().equals("COL_ALIAS"));

		assertTrue(field2.getName().equals("*"));
		assertTrue(field2.getIndex().equals("TABLE_NAME"));
		assertTrue(field2.getAlias().equals("COL_ALIAS"));
		assertTrue(field8.getName().equals("\"FK_NAME.*\""));
		assertTrue(field8.getIndex().equals("TABLE_NAME"));
		assertTrue(field8.getAlias().equals("COL_ALIAS"));

		assertTrue(field3.getName().equals("*"));
		assertTrue(field3.getIndex().equals("TABLE_NAME"));
		assertNull(field3.getAlias());		
		assertTrue(field9.getName().equals("\"FK_NAME.*\""));
		assertTrue(field9.getIndex().equals("TABLE_NAME"));
		assertNull(field9.getAlias());

		assertTrue(field4.getName().equals("*"));
		assertNull(field4.getIndex());
		assertNull(field4.getAlias());
		assertTrue(field10.getName().equals("\"FK_NAME.*\""));
		assertNull(field10.getIndex());
		assertNull(field10.getAlias());

		assertTrue(field5.getName().equals("*"));
		assertNull(field5.getIndex());
		assertTrue(field5.getAlias().equals("COL_ALIAS"));
		assertTrue(field11.getName().equals("\"FK_NAME.*\""));
		assertNull(field11.getIndex());
		assertTrue(field11.getAlias().equals("COL_ALIAS"));

		assertTrue(field6.getName().equals("*"));
		assertNull(field6.getIndex());
		assertTrue(field6.getAlias().equals("COL_ALIAS"));		
		assertTrue(field12.getName().equals("\"FK_NAME.*\""));
		assertNull(field12.getIndex());
		assertTrue(field12.getAlias().equals("COL_ALIAS"));
	}
	
	
	@Test
	@Ignore
	public void parse () throws SQLException, JSQLParserException {
		String sql = "SELECT WTH.\"WITHDRAWAL.ID\"    ID_TX, STATE_TYPE.PIPPO     AS    PLUTO FROM A2E_TX_WITHDRAWAL WITHDRAWAL INNER JOIN A2E_TX_STATE_TYPE STATE_TYPE ON STATE_TYPE.ID_TX_STATE_TYPE = WITHDRAWAL.FK_TX_STATE_TYPE";
		String sql1 = "SELECT * FROM A2E_TX_WITHDRAWAL";
		String sql2 = "SELECT withdrawal.pfk_transaction, state_type.str_description, tr.id_transaction FROM A2E_TX_WITHDRAWAL WITHDRAWAL INNER JOIN A2E_TX_STATE_TYPE STATE_TYPE ON STATE_TYPE.ID_TX_STATE_TYPE = WITHDRAWAL.FK_TX_STATE_TYPE INNER JOIN A2E_TRANSACTION TR ON tr.id_transaction = withdrawal.pfk_transaction";
		String sql3 = "Select a from pippo, (select b from pluto) t1 where pippo.a = t1.b";
		
		ParsedQuery qry = SqlParser.parse(sql);
		ParsedQuery qry1 = SqlParser.parse(sql1);
		
		assertTrue(qry.getFields().size()==2);
		assertTrue(qry.getFields().get(0).getName().equals("\"WITHDRAWAL.ID\""));
		assertTrue(qry.getFields().get(0).getIndex().equals("wth"));
		assertTrue(qry.getFields().get(0).getAlias().equals("ID_TX"));
		assertTrue(qry.getFields().get(1).getName().equals("PIPPO"));
		assertTrue(qry.getFields().get(1).getIndex().equals("STATE_TYPE"));
		assertTrue(qry.getFields().get(1).getAlias().equals("PLUTO"));
		
		
		assertTrue(qry1.getFields().size()==1);
		assertTrue(qry1.getFields().get(0).getName().equals("*"));		
		assertNull(qry1.getFields().get(0).getIndex());
		assertNull(qry1.getFields().get(0).getAlias());
	}
	
	@Test
	@Ignore
	public void parseBeetwenConditionQuery () throws SQLException, JSQLParserException {
		// given		
		String sql = "SELECT T1.COL10 PrimaColonna,T1.COL11 AS  SecondaColonna \n" +
					 "FROM TABLE1 T1 \n " + 
					 "WHERE T1.COL1 Between '01-01-2020' AND '31-12-2020'";
		
		// when
		ParsedQuery qry = SqlParser.parse(sql);
		
		//then
		assertTrue(qry.getFields().size()==2);
		assertTrue(qry.getFields().get(0).getName().equals("COL10"));	
		assertTrue(qry.getFields().get(0).getIndex().equals("T1"));
		assertTrue(qry.getFields().get(0).getAlias().equals("PrimaColonna"));	
		assertTrue(qry.getFields().get(1).getName().equals("COL11"));	
		assertTrue(qry.getFields().get(1).getIndex().equals("T1"));
		assertTrue(qry.getFields().get(1).getAlias().equals("SecondaColonna"));
		assertTrue(qry.getIndex().getName().equals("TABLE1"));
		assertTrue(qry.getIndex().getAlias().equals("T1"));		
		assertTrue(qry.getIndex().getJoinedIndex().size()==0);
		
		assertTrue(qry.getIndex().getCondition().get(0).getCondition().compareTo(ECondition.Between)==0);
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getName().equals("COL1"));
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getIndex().equals("T1"));
		assertNull(qry.getIndex().getCondition().get(0).getLeftField().getAlias());
		assertTrue(qry.getIndex().getCondition().get(0).getLeftField().getValue().equals("5,10,15,20"));
		
	}
}
