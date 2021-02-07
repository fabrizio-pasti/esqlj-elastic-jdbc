package org.fpasti.jdbc.esqlj.support;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class EsRuntimeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public EsRuntimeException(String message) {
		super(message);
	}
}
