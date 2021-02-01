package org.takeshi.jdbc.esqlj.elastic.query.statement.model;

import java.util.Arrays;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnalyticType;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.AnyType;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OrderByClause;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.PartitionByClause;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.expression.WindowOffset;
import net.sf.jsqlparser.expression.WindowRange;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.FullTextSearch;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperatorType;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.expression.operators.relational.SupportsOldOracleJoinSyntax;
import net.sf.jsqlparser.schema.Column;

public enum ExpressionEnum {
		ALIAS(Alias.class),
		ALL_COMPARISON_EXPRESSION(AllComparisonExpression.class),
		ANALYTIC_EXPRESSION(AnalyticExpression.class),
		ANALYTIC_TYPE(AnalyticType.class),
		ANY_COMPARISON_EXPRESSION(AnyComparisonExpression.class),
		ANY_TYPE(AnyType.class),
		ARRAY_EXPRESSION(ArrayExpression.class),
		BINARY_EXPRESSION(BinaryExpression.class),
		CASE_EXPRESSION(CaseExpression.class),
		CAST_EXPRESSION(CastExpression.class),
		COLLATE_EXPRESSION(CollateExpression.class),
		COLUMN(Column.class),
		DATE_TIME_LITERAL_EXPRESSION(DateTimeLiteralExpression.class),
		DATE_VALUE(DateValue.class),
		DOUBLE_VALUE(DoubleValue.class),
		EXPRESSION(Expression.class),
		EXPRESSION_VISITOR(ExpressionVisitor.class),
		EXPRESSION_VISITOR_ADAPTER(ExpressionVisitorAdapter.class),
		EXTRACT_EXPRESSION(ExtractExpression.class),
		FUNCTION(Function.class),
		HEX_VALUE(HexValue.class),
		INTERVAL_EXPRESSION(IntervalExpression.class),
		JDBC_NAMED_PARAMETER(JdbcNamedParameter.class),
		JDBC_PARAMETER(JdbcParameter.class),
		JSON_EXPRESSION(JsonExpression.class),
		KEEP_EXPRESSION(KeepExpression.class),
		LONG_VALUE(LongValue.class),
		NEXT_VAL_EXPRESSION(NextValExpression.class),
		NOT_EXPRESSION(NotExpression.class),
		NULL_VALUE(NullValue.class),
		NUMERIC_BIND(NumericBind.class),
		ORDER_BY_CLAUSE(OrderByClause.class),
		PARENTHESIS(Parenthesis.class),
		PARTITION_BY_CLAUSE(PartitionByClause.class),
		ROW_CONSTRUCTOR(RowConstructor.class),
		SIGNED_EXPRESSION(SignedExpression.class),
		STRING_VALUE(StringValue.class),
		TIME_KEY_EXPRESSION(TimeKeyExpression.class),
		TIMESTAMP_VALUE(TimestampValue.class),
		TIME_VALUE(TimeValue.class),
		USER_VARIABLE(UserVariable.class),
		VALUE_LIST_EXPRESSION(ValueListExpression.class),
		WHEN_CLAUSE(WhenClause.class),
		WINDOW_ELEMENT(WindowElement.class),
		WINDOW_OFFSET(WindowOffset.class),
		WINDOW_RANGE(WindowRange.class),
		ADDITION(Addition.class),
		BITWISE_AND(BitwiseAnd.class),
		BITWISE_LEFT_SHIFT(BitwiseLeftShift.class),
		BITWISE_OR(BitwiseOr.class),
		BITWISE_RIGHT_SHIFT(BitwiseRightShift.class),
		BITWISE_XOR(BitwiseXor.class),
		CONCAT(Concat.class),
		DIVISION(Division.class),
		INTEGER_DIVISION(IntegerDivision.class),
		MODULO(Modulo.class),
		MULTIPLICATION(Multiplication.class),
		SUBTRACTION(Subtraction.class),
		AND_EXPRESSION(AndExpression.class),
		OR_EXPRESSION(OrExpression.class),
		BETWEEN(Between.class),
		COMPARISON_OPERATOR(ComparisonOperator.class),
		EQUALS_TO(EqualsTo.class),
		EXIST_SEXPRESSION(ExistsExpression.class),
		EXPRESSION_LIST(ExpressionList.class),
		FULL_TEXT_SEARCH(FullTextSearch.class),
		GREATER_THAN(GreaterThan.class),
		GREATER_THAN_EQUALS(GreaterThanEquals.class),
		IN_EXPRESSION(InExpression.class),
		IS_BOOLEAN_EXPRESSION(IsBooleanExpression.class),
		IS_NULL_EXPRESSION(IsNullExpression.class),
		ITEMS_LIST(ItemsList.class),
		ITEMS_LIST_VISITOR(ItemsListVisitor.class),
		ITEMS_LIST_VISITOR_ADAPTER(ItemsListVisitorAdapter.class),
		JSON_OPERATOR(JsonOperator.class),
		LIKE_EXPRESSION(LikeExpression.class),
		MATCHES(Matches.class),
		MINOR_THAN(MinorThan.class),
		MINOR_THAN_EQUALS(MinorThanEquals.class),
		MULTI_EXPRESSION_LIST(MultiExpressionList.class),
		NAMED_EXPRESSION_LIST(NamedExpressionList.class),
		NOT_EQUALS_TO(NotEqualsTo.class),
		OLD_ORACLE_JOIN_BINARY_EXPRESSION(OldOracleJoinBinaryExpression.class),
		REG_EXP_MATCH_OPERATOR(RegExpMatchOperator.class),
		REG_EXP_MATCH_OPERATOR_TYPE(RegExpMatchOperatorType.class),
		SIMILAR_TO_EXPRESSION(SimilarToExpression.class),
		SUPPORTS_OLD_ORACLE_JOIN_SYNTAX(SupportsOldOracleJoinSyntax.class);
		
		Class<?> clazz;
		
		ExpressionEnum(Class<?> clazz) {
			this.clazz = clazz;
		}
		
		@SuppressWarnings("rawtypes")
		public Class getClazz() {
			return clazz;
		}
		
		public static ExpressionEnum resolveByInstance(Object expression) {
			return Arrays.stream(ExpressionEnum.values()).filter(expr -> expr.getClazz().equals(expression.getClass())).findFirst()
					.orElseGet(null);
		}
		
	}