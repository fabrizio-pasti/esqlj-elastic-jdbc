package org.fpasti.jdbc.esqlj.support;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class EsWrapException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	private Exception wrappedException;
	
	public EsWrapException(Exception wrappedExceptrion) {
		super(wrappedExceptrion.getMessage());
		this.wrappedException = wrappedExceptrion;
	}
	
	public Exception getWrappException() {
		return wrappedException;
	}
}
