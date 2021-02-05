package org.fpasti.jdbc.esqlj.elastic.query.impl.search.model;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class EvaluateQueryResult {
		private boolean and = true;
		private List<QueryBuilder> queryBuilders = new ArrayList<QueryBuilder>();
		private List<QueryBuilder> notQueryBuilders = new ArrayList<QueryBuilder>();
		private TermsQuery termsQuery = new TermsQuery();
		
		public EvaluateQueryResult() {
		}

		public EvaluateQueryResult(QueryBuilder queryBuilder) {
			addQueryBuilder(queryBuilder);
		}

		public List<QueryBuilder> getQueryBuilders() {
			return queryBuilders;
		}

		public List<QueryBuilder> getNotQueryBuilders() {
			return notQueryBuilders;
		}

		public void addQueryBuilder(QueryBuilder queryBuilder) {
			queryBuilders.add(queryBuilder);
		}
		
		public boolean isListEmpty() {
			return queryBuilders.size() == 0;
		}

		public boolean isNotListEmpty() {
			return notQueryBuilders.size() == 0;
		}

		public boolean isTermsEmpty() {
			return termsQuery.isEmpty();
		}

		public TermsQuery getTermsQuery() {
			return termsQuery;
		}
		
		public void setEqualTerm(String term, Object value) {
			termsQuery.addEqualObject(term, value);
		}

		public void setNotEqualTerm(String term, Object value) {
			termsQuery.addNotEqualObject(term, value);
		}

		public EvaluateQueryResult merge(boolean and, EvaluateQueryResult resAndRight) {
			this.and = and;
			queryBuilders.addAll(resAndRight.getQueryBuilders());
			notQueryBuilders.addAll(resAndRight.getNotQueryBuilders());
			termsQuery.merge(resAndRight.getTermsQuery());
			return this;
		}
		
		
		public EvaluateQueryResultType getType() {
			if(queryBuilders.size() == 1 && isNotListEmpty() && isTermsEmpty()) {
				return EvaluateQueryResultType.ONLY_ONE;
			}
			
			if(isListEmpty() && notQueryBuilders.size() == 1 && isTermsEmpty()) {
				return EvaluateQueryResultType.ONLY_ONE_NOT;
			}
			
			if(isListEmpty() && isNotListEmpty() && !isTermsEmpty()) {
				if(termsQuery.getEqualObjects().size() == 1 && termsQuery.getNotEqualObjects().isEmpty()) {
					return EvaluateQueryResultType.ONLY_ONE_TERMS;
				}
				
				if(termsQuery.getEqualObjects().isEmpty() && termsQuery.getNotEqualObjects().size() == 1) {
					return EvaluateQueryResultType.ONLY_ONE_NOT_TERMS;
				}
			}
			
			return EvaluateQueryResultType.MIXED;

		}

		public boolean isAnd() {
			return and;
		}
	
	}