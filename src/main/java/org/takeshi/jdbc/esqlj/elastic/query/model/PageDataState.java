package org.takeshi.jdbc.esqlj.elastic.query.model;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public enum PageDataState {
	NOT_INITIALIZED,
	READY_TO_ITERATE,
	ITERATION_STARTED,
	ITERATION_FINISHED
}
