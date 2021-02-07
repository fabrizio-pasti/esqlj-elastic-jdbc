package org.fpasti.jdbc.esqlj.testUtils;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.fpasti.jdbc.esqlj.EsConnection;

public class ElasticTestService {

	private static final String RESOURCES_DOCUMENTS = "documents";
	private static final String RESOURCES_TEST_INDEX_TEMPLATE_JSON = "test-index-template.json";
	private static final String ESQLJ_TEST_TEMPLATE = "esqlj-test-template";
	private static final String ELASTIC_BASE_INDEX_CREATE_AND_DESTROY = "esqlj-test-volatile-";
	private static final String ELASTIC_BASE_INDEX_CREATE_ONLY = "esqlj-test-static-010";
	
	public static String CURRENT_INDEX;
	private static Integer NUMBER_OF_DOCS;
	
	public static void setup(EsConnection connection, boolean createAndDestroy) throws Exception {
		cleanUp(connection.getElasticClient());
		setCurrentIndex(createAndDestroy);
		boolean createTemplateAndPostDocs = createAndDestroy ? true : !checkIfStaticIndexJustPresent(connection.getElasticClient());
		if(createTemplateAndPostDocs) {
			addIndexTemplate(connection.getElasticClient());
			postDocuments(connection.getElasticClient(), createAndDestroy);
		}
		
	}

	public static void tearOff(EsConnection connection) throws IOException {
		cleanUp(connection.getElasticClient());
	}

	private static void cleanUp(RestHighLevelClient client) throws IOException {
		DeleteIndexRequest requestDeleteIndex = new DeleteIndexRequest(ELASTIC_BASE_INDEX_CREATE_AND_DESTROY.concat("*"));        
		client.indices().delete(requestDeleteIndex, RequestOptions.DEFAULT);
		
		IndexTemplatesExistRequest request = new IndexTemplatesExistRequest(ESQLJ_TEST_TEMPLATE);
		boolean indexTemplateExists = client.indices().existsTemplate(request, RequestOptions.DEFAULT);
		
		if(indexTemplateExists) {
			DeleteIndexTemplateRequest requestDeleteTemplate = new DeleteIndexTemplateRequest();
			requestDeleteTemplate.name(ESQLJ_TEST_TEMPLATE);
			client.indices().deleteTemplate(requestDeleteTemplate, RequestOptions.DEFAULT);
		}
	}

	private static void addIndexTemplate(RestHighLevelClient client) throws Exception {
		PutIndexTemplateRequest request = new PutIndexTemplateRequest(ESQLJ_TEST_TEMPLATE);
		request.source(TestUtils.getResourceAsText(RESOURCES_TEST_INDEX_TEMPLATE_JSON), XContentType.JSON);
		AcknowledgedResponse res = client.indices().putTemplate(request, RequestOptions.DEFAULT);
		if(!res.isAcknowledged()) {
			throw new Exception("Failed to put test template on Elastic instance");
		}
	}
	
	private static void postDocuments(RestHighLevelClient client, boolean createAndDestroy) throws Exception {
		for(File file : TestUtils.listFiles(RESOURCES_DOCUMENTS)) {
			postDocument(client, file.getName().replace(".json", ""), TestUtils.readFile(file), createAndDestroy);
		}
	}

	private static void postDocument(RestHighLevelClient client, String id, String body, boolean createAndDestroy) throws Exception {
		IndexRequest request = new IndexRequest(CURRENT_INDEX); 
		request.id(id);
		request.source(body, XContentType.JSON);
		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
		if(indexResponse.getShardInfo().getFailed() > 0) {
			throw new Exception("Failed to insert test document on Elastic instance");
		}
	}
	
	private static void setCurrentIndex(boolean createAndDestroy) {
		CURRENT_INDEX = createAndDestroy ? String.format("%s.%s", ELASTIC_BASE_INDEX_CREATE_AND_DESTROY, UUID.randomUUID()) : ELASTIC_BASE_INDEX_CREATE_ONLY;
	}

	private static boolean checkIfStaticIndexJustPresent(RestHighLevelClient client) throws IOException {
		GetIndexRequest request = new GetIndexRequest(ELASTIC_BASE_INDEX_CREATE_ONLY);
		return client.indices().exists(request, RequestOptions.DEFAULT);
	}

	public static int getNumberOfDocs() {
		if(NUMBER_OF_DOCS == null) {
			CountRequest countRequest = new CountRequest(CURRENT_INDEX);
			CountResponse res;
			try {
				res = TestUtils.getLiveConnection().getElasticClient().count(countRequest, RequestOptions.DEFAULT);
			} catch (Exception e) {
				throw new RuntimeException("Failed to get number of documents on testing index");
			} 
			NUMBER_OF_DOCS = new Long(res.getCount()).intValue();
		}
		return NUMBER_OF_DOCS;
	}
}
