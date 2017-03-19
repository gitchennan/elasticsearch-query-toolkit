package org.elasticsearch.jdbc.search;

import org.elasticsearch.action.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSearchActionExecutor {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSearchActionExecutor.class);

    private static final JdbcSearchActionExecutor JDBC_SEARCH_ACTION_EXECUTOR = new JdbcSearchActionExecutor();

    private JdbcSearchActionExecutor() {

    }

    public static JdbcSearchActionExecutor get() {
        return JDBC_SEARCH_ACTION_EXECUTOR;
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
                logger.debug(String.format("[Search_Request] %s", requestBuilder.toString()));
                logger.debug(String.format("[Search_Response] %s", response.toString()));
            }

            @Override
            public void onFailure(Throwable throwable) {
                logger.error("Execute search req error!", throwable);
            }
        };
    }

}
