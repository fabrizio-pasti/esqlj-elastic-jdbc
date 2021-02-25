package org.fpasti.jdbc.esqlj.elastic.query.statement.formatter;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.FunctionEnum;

import net.sf.jsqlparser.expression.Function;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class FormatterFactory {

	public static Formatter getFormatter(FunctionEnum functionType, Function function) {
		if(function == null) {
			return null;
		}
		
		switch(functionType) {
			case TO_CHAR:
				return new ToCharFormatter(function);
			case LATITUDE:
				return new LatitudeFormatter(function);
			case LONGITUDE:
				return new LongitudeFormatter(function);
			default:
				return null; 
		}
	}
}
