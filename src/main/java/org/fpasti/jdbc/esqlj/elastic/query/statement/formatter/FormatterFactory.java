package org.fpasti.jdbc.esqlj.elastic.query.statement.formatter;

import net.sf.jsqlparser.expression.Function;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class FormatterFactory {

	public static Formatter getFormatter(Function function) {
		if(function == null) {
			return null;
		}
		
		switch(function.getName().toUpperCase()) {
			case "TO_CHAR":
				return new ToCharFormatter(function);
			default:
				return null; 
		}
	}
}
