package org.takeshi.jdbc.esqlj.elastic.query.model;

import java.sql.RowId;

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
