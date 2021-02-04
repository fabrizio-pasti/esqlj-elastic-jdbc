package org.takeshi.jdbc.esqlj.elastic.model;

import java.sql.RowId;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class EsRowId implements RowId {

	private String docId;
	
	public EsRowId(String docId) {
		this.docId = docId;
	}
	
	@Override
	public byte[] getBytes() {
		return docId.getBytes();
	}

}
