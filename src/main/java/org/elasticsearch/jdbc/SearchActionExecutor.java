package org.elasticsearch.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.*;

public class SearchActionExecutor {

    private static final Logger logger = LogManager.getLogger(SearchActionExecutor.class);

    private static final SearchActionExecutor searchActionExecutor = new SearchActionExecutor();

    private SearchActionExecutor() {

    }

    public static SearchActionExecutor get() {
        return searchActionExecutor;
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> Response syncExecuteWithException(RequestBuilder requestBuilder) {
        ListenableActionFuture<Response> searchActionFuture = requestBuilder.execute();
        searchActionFuture.addListener(defaultActionListener(requestBuilder));
        return searchActionFuture.actionGet();
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> Response syncExecute(RequestBuilder requestBuilder) {
        try {
            ListenableActionFuture<Response> searchActionFuture = requestBuilder.execute();
            searchActionFuture.addListener(defaultActionListener(requestBuilder));
            return searchActionFuture.actionGet();
        }
        catch (Exception ex) {
            return null;
        }
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void asyncExecute(RequestBuilder requestBuilder) {
        ListenableActionFuture<Response> searchActionFuture = requestBuilder.execute();
        searchActionFuture.addListener(defaultActionListener(requestBuilder));
    }

    private <Response extends ActionResponse> ActionListener<Response> defaultActionListener(ActionRequestBuilder requestBuilder) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                System.out.println((String.format("[Search_Request] %s", requestBuilder.toString())));
                //logger.debug(String.format("[Search_Request] %s", requestBuilder.toString()));
                //logger.debug(String.format("[Search_Response] %s", response.toString()));
            }

            @Override
            public void onFailure(Throwable throwable) {

            }
        };
    }

}
