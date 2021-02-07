package org.fpasti.jdbc.esqlj.elastic.query.impl.search.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class TermsQuery {
	private Map<String, List<Object>> equalObjects = new HashMap<String, List<Object>>();
	private Map<String, List<Object>> notEqualObjects = new HashMap<String, List<Object>>();

	public void addEqualObject(String field, Object eqO) {
		if (!equalObjects.containsKey(field)) {
			equalObjects.put(field, new ArrayList<Object>());
		}
		equalObjects.get(field).add(eqO);
	}

	public void addNotEqualObject(String field, Object nEqO) {
		if (!notEqualObjects.containsKey(field)) {
			notEqualObjects.put(field, new ArrayList<Object>());
		}
		notEqualObjects.get(field).add(nEqO);
	}

	public Map<String, List<Object>> getEqualObjects() {
		return equalObjects;
	}

	public Map<String, List<Object>> getNotEqualObjects() {
		return notEqualObjects;
	}

	public void merge(TermsQuery termsQuery) {
		equalObjects = mergeMap(equalObjects, termsQuery.getEqualObjects());
		notEqualObjects = mergeMap(notEqualObjects, termsQuery.getNotEqualObjects());
	}

	private Map<String, List<Object>> mergeMap(Map<String, List<Object>> a, Map<String, List<Object>> b) {
		return Stream.of(a, b).flatMap(map -> map.entrySet().stream())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> {
					v1.addAll(v2);
					return v1;
				}));
	}

	public boolean isEmpty() {
		return equalObjects.size() == 0 && notEqualObjects.size() == 0;
	}
}