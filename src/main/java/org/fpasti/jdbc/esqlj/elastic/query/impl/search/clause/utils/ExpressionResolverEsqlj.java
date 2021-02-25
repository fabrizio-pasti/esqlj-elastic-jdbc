package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.utils;

import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.model.EvaluateQueryResult;
import org.fpasti.jdbc.esqlj.support.EsqljConstants;
import org.fpasti.jdbc.esqlj.support.Utils;

import net.sf.jsqlparser.expression.Function;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ExpressionResolverEsqlj {

	public static EvaluateQueryResult manageExpression(Function function) throws SQLSyntaxErrorException {
		String queryType = function.getName().toUpperCase();
		List<Object> arguments = function.getParameters().getExpressions().stream().map(param -> ExpressionResolverValue.evaluateValueExpression(param)).collect(Collectors.toList());
		
		switch(queryType) {
			case "QUERY_STRING":
				return queryString(queryType, arguments);
			case "GEO_BOUNDING_BOX":
				return geoBoundingBox(queryType, arguments);
			default:
				throw new SQLSyntaxErrorException(String.format("Unsupported function: '%s'", function.toString()));
		}
	}

	private static EvaluateQueryResult queryString(String queryType, List<Object> arguments) throws SQLSyntaxErrorException {
		checkMinimumNumberOfParameters(queryType, arguments, 2);
		String query = arguments.get(0).toString();
		String[] fields = arguments.get(1).toString().split(",");
		
		QueryStringQueryBuilder qsqb = QueryBuilders.queryStringQuery(query);
		Arrays.asList(fields).forEach(field -> {
			qsqb.field(field);
			});
		
		arguments.stream().skip(2).forEach(param -> {
			Utils.setAttributeInElasticObject(qsqb, getParameterName(param.toString()), getParameterValue(param.toString()));
		});
	
		return new EvaluateQueryResult(qsqb);
	}
	
	private static EvaluateQueryResult geoBoundingBox(String queryType, List<Object> arguments) throws SQLSyntaxErrorException {
		checkExactNumberOfParameters(queryType, arguments, 5);
		
		GeoBoundingBoxQueryBuilder builder = QueryBuilders.geoBoundingBoxQuery(arguments.get(0).toString());
		builder.setCorners(new GeoPoint(convertToDouble(arguments.get(1)), convertToDouble(arguments.get(2))), new GeoPoint(convertToDouble(arguments.get(3)), convertToDouble(arguments.get(4))));
		return new EvaluateQueryResult(builder);
	}
	
	private static double convertToDouble(Object value) {
		if(value instanceof Double) {
			return (double)value;
		}
		
		if(value instanceof Integer) {
			return new Double((int)value);
		}
		
		return new Double((long)value);
	}
	
	private static String getParameterName(String param) {
		return param.split(":")[0];
	}

	private static String getParameterValue(String param) {
		return param.split(":")[1];
	}

	private static void checkExactNumberOfParameters(String queryType, List<Object> arguments, int numMinNumOfArguments) throws SQLSyntaxErrorException {
		if(arguments.size() != numMinNumOfArguments) {
			throw new SQLSyntaxErrorException(String.format("%s ::%s requires %d parameters", EsqljConstants.ESQLJ_WHERE_CLAUSE, queryType, numMinNumOfArguments));
		}
	}
	
	private static void checkMinimumNumberOfParameters(String queryType, List<Object> arguments, int numMinNumOfArguments) throws SQLSyntaxErrorException {
		if(arguments.size() < numMinNumOfArguments) {
			throw new SQLSyntaxErrorException(String.format("%s ::%s requires at least %d parameters", EsqljConstants.ESQLJ_WHERE_CLAUSE, queryType, numMinNumOfArguments));
		}
	}
}
