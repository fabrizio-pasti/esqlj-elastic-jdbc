package org.takeshi.jdbc.esqlj.testUtils;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ElasticLiveEnvironment implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

    private static boolean initialized = false;

    synchronized private static void initialize(ExtensionContext context, Object instance) throws Exception {
        if (!initialized) {
            initialized = true;
            context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL).put("ElasticLiveEnvironment", instance);
            TestUtils.setupElastic();
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        initialize(context, this);
    }

    @Override
    public void close() throws Exception {
    	TestUtils.tearOffElastic();
    }

}