package org.takeshi.jdbc.esqlj.parser;

import java.sql.SQLException;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import net.sf.jsqlparser.JSQLParserException;

@RunWith(JUnitParamsRunner.class)
public class SqlParserTest {

	@Test
	public void parse () throws SQLException, JSQLParserException {
		String sql = "SELECT wth.\"WITHDRAWAL.ID\"    ID_TX, STATE_TYPE.PIPPO     AS    PLUTO FROM A2E_TX_WITHDRAWAL WITHDRAWAL INNER JOIN A2E_TX_STATE_TYPE STATE_TYPE ON STATE_TYPE.ID_TX_STATE_TYPE = WITHDRAWAL.FK_TX_STATE_TYPE";
		String sql1 = "SELECT * FROM A2E_TX_WITHDRAWAL";
		String sql2 = "SELECT withdrawal.pfk_transaction, state_type.str_description, tr.id_transaction FROM A2E_TX_WITHDRAWAL WITHDRAWAL INNER JOIN A2E_TX_STATE_TYPE STATE_TYPE ON STATE_TYPE.ID_TX_STATE_TYPE = WITHDRAWAL.FK_TX_STATE_TYPE INNER JOIN A2E_TRANSACTION TR ON tr.id_transaction = withdrawal.pfk_transaction";
		String sql3 = "Select a from pippo, (select b from pluto) t1 where pippo.a = t1.b";
		
		
		SqlParser.parse(sql);
	}
	
	@Test
	public void parseFiels () throws SQLException, JSQLParserException {
		String fieldValue1 = "STATE_TYPE.ID_STATE_TYPE AS ID_STATE";
		String filedValue2 = "WITHDRAWAL.PFK_WITHDRAWAL ID_WITHDRAWAL";
		String filedValue3 = "WITHDRAWAL.PFK_WITHDRAWAL";
		String filedValue4 = "PFK_WITHDRAWAL";
		String filedValue5 = "PFK_WITHDRAWAL ID_WITHDRAWAL";
		String filedValue6 = "PFK_WITHDRAWAL AS ID_WITHDRAWAL";
		
		SqlParser.parseField(fieldValue1);
		
	}
}
