package org.elasticsearch.jdbc;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ListenableActionFuture;

public class SearchActionExecutor {

    private static final SearchActionExecutor searchActionExecutor = new SearchActionExecutor();

    private SearchActionExecutor() {

    }

    public static SearchActionExecutor get() {
        return searchActionExecutor;
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> Response syncExecuteWithException(RequestBuilder requestBuilder) {
        ListenableActionFuture<Response> searchActionFuture = requestBuilder.execute();
        return searchActionFuture.actionGet();
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> Response syncExecute(RequestBuilder requestBuilder) {
        try {
            ListenableActionFuture<Response> searchActionFuture = requestBuilder.execute();
            return searchActionFuture.actionGet();
        }
        catch (Exception ex) {
            return null;
        }
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void asyncExecute(RequestBuilder requestBuilder) {
        requestBuilder.execute();
    }
}
