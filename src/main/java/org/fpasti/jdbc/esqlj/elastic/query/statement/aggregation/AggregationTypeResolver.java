package org.fpasti.jdbc.esqlj.elastic.query.statement.aggregation;

import java.sql.SQLSyntaxErrorException;

import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.fpasti.jdbc.esqlj.support.EsWrapException;

import net.sf.jsqlparser.expression.Function;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class AggregationTypeResolver {

	public static ElasticFieldType resolveAggregationType(Function function) {
		switch(function.getMultipartName().get(0)) {
			case "COUNT":
				return ElasticFieldType.LONG;
		}
		
		throw new EsWrapException(new SQLSyntaxErrorException(String.format("Unsupported aggregating function '%s'", function.getName())));
	}
}
