package org.takeshi.jdbc.esqlj.elastic.query.statement.formatter;

import java.sql.SQLSyntaxErrorException;

import org.takeshi.jdbc.esqlj.support.EsRuntimeException;

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
		}
		
		throw new EsRuntimeException(String.format("Unsupported select function '%s'", function.getName()));
	}
}
