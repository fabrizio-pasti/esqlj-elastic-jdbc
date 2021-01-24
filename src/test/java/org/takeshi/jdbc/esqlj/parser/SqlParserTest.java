package org.takeshi.jdbc.esqlj.parser;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.takeshi.jdbc.esqlj.parser.model.Field;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

import junitparams.JUnitParamsRunner;
import net.sf.jsqlparser.JSQLParserException;

@RunWith(JUnitParamsRunner.class)
public class SqlParserTest {

	@Test
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
	public void parseFiels () throws SQLException, JSQLParserException {
		// To do
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
		
		String fieldValue13 = "TABLE_NAME.* AS COL_ALIAS";
		String fieldValue14 = "TABLE_NAME.* COL_ALIAS";
		String fieldValue15 = "TABLE_NAME.*";
		String fieldValue16 = "*";
		String fieldValue17 = "* COL_ALIAS";
		String fieldValue18 = "* AS COL_ALIAS";
		
		String fieldValue19 = "TABLE_NAME.\"FK_NAME.*\" AS COL_ALIAS";
		String fieldValue20 = "TABLE_NAME.\"FK_NAME.*\" COL_ALIAS";
		String fieldValue21 = "TABLE_NAME.\"FK_NAME.*\"";
		String fieldValue22 = "\"FK_NAME.*\"";
		String fieldValue23 = "\"FK_NAME.*\" COL_ALIAS";
		String fieldValue24 = "\"FK_NAME.*\" AS COL_ALIAS";
				
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
		Field  field13 = SqlParser.parseField(fieldValue13);
		
		Field  field14 = SqlParser.parseField(fieldValue14);
		Field  field15 = SqlParser.parseField(fieldValue15);
		Field  field16 = SqlParser.parseField(fieldValue16);
		Field  field17 = SqlParser.parseField(fieldValue17);
		Field  field18 = SqlParser.parseField(fieldValue18);
		Field  field19 = SqlParser.parseField(fieldValue19);
		Field  field20 = SqlParser.parseField(fieldValue20);
		Field  field21 = SqlParser.parseField(fieldValue21);
		Field  field22 = SqlParser.parseField(fieldValue22);
		Field  field23 = SqlParser.parseField(fieldValue23);
		Field  field24 = SqlParser.parseField(fieldValue24);
		
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
		
		assertTrue(field13.getName().equals("*"));
		assertTrue(field13.getIndex().equals("TABLE_NAME"));
		assertTrue(field13.getAlias().equals("COL_ALIAS"));
		assertTrue(field19.getName().equals("\"FK_NAME.*\""));
		assertTrue(field19.getIndex().equals("TABLE_NAME"));
		assertTrue(field19.getAlias().equals("COL_ALIAS"));
		
		assertTrue(field14.getName().equals("*"));
		assertTrue(field14.getIndex().equals("TABLE_NAME"));
		assertTrue(field14.getAlias().equals("COL_ALIAS"));
		assertTrue(field20.getName().equals("\"FK_NAME.*\""));
		assertTrue(field20.getIndex().equals("TABLE_NAME"));
		assertTrue(field20.getAlias().equals("COL_ALIAS"));

		assertTrue(field15.getName().equals("*"));
		assertTrue(field15.getIndex().equals("TABLE_NAME"));
		assertNull(field15.getAlias());		
		assertTrue(field21.getName().equals("\"FK_NAME.*\""));
		assertTrue(field21.getIndex().equals("TABLE_NAME"));
		assertNull(field21.getAlias());

		assertTrue(field16.getName().equals("*"));
		assertNull(field16.getIndex());
		assertNull(field16.getAlias());
		assertTrue(field22.getName().equals("\"FK_NAME.*\""));
		assertNull(field22.getIndex());
		assertNull(field22.getAlias());

		assertTrue(field17.getName().equals("*"));
		assertNull(field17.getIndex());
		assertTrue(field17.getAlias().equals("COL_ALIAS"));
		assertTrue(field23.getName().equals("\"FK_NAME.*\""));
		assertNull(field23.getIndex());
		assertTrue(field23.getAlias().equals("COL_ALIAS"));

		assertTrue(field18.getName().equals("*"));
		assertNull(field18.getIndex());
		assertTrue(field18.getAlias().equals("COL_ALIAS"));		
		assertTrue(field24.getName().equals("\"FK_NAME.*\""));
		assertNull(field24.getIndex());
		assertTrue(field24.getAlias().equals("COL_ALIAS"));
	}
}
