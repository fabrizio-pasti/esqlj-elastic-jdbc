package org.takeshi.jdbc.esqlj.test;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ElasticLiveUnitCondition implements ExecutionCondition {
   @Override
   public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      String config = System.getenv(TestUtils.ENV_PROP_ESQLJ_TEST_CONFIG);
      return config == null ? ConditionEvaluationResult.disabled("Test disable: missing ESQLJ_TEST_CONFIG system property") : ConditionEvaluationResult.enabled("Test enabled");  
   }
}