package org.es.sql.utils;

import org.elasticsearch.action.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequestBuilder;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldstats.FieldStatsRequest;
import org.elasticsearch.action.fieldstats.FieldStatsRequestBuilder;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.termvectors.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

public class ElasticMockClient implements Client {

    private static final Client mockClient = new ElasticMockClient();

    public static Client get() {
        return mockClient;
    }

    @Override
    public AdminClient admin() {
        return null;
    }

    @Override
    public ActionFuture<IndexResponse> index(IndexRequest indexRequest) {
        return null;
    }

    @Override
    public void index(IndexRequest indexRequest, ActionListener<IndexResponse> actionListener) {

    }

    @Override
    public IndexRequestBuilder prepareIndex() {
        return null;
    }

    @Override
    public ActionFuture<UpdateResponse> update(UpdateRequest updateRequest) {
        return null;
    }

    @Override
    public void update(UpdateRequest updateRequest, ActionListener<UpdateResponse> actionListener) {

    }

    @Override
    public UpdateRequestBuilder prepareUpdate() {
        return null;
    }

    @Override
    public UpdateRequestBuilder prepareUpdate(String s, String s1, String s2) {
        return null;
    }

    @Override
    public IndexRequestBuilder prepareIndex(String s, String s1) {
        return null;
    }

    @Override
    public IndexRequestBuilder prepareIndex(String s, String s1, String s2) {
        return null;
    }

    @Override
    public ActionFuture<DeleteResponse> delete(DeleteRequest deleteRequest) {
        return null;
    }

    @Override
    public void delete(DeleteRequest deleteRequest, ActionListener<DeleteResponse> actionListener) {

    }

    @Override
    public DeleteRequestBuilder prepareDelete() {
        return null;
    }

    @Override
    public DeleteRequestBuilder prepareDelete(String s, String s1, String s2) {
        return null;
    }

    @Override
    public ActionFuture<BulkResponse> bulk(BulkRequest bulkRequest) {
        return null;
    }

    @Override
    public void bulk(BulkRequest bulkRequest, ActionListener<BulkResponse> actionListener) {

    }

    @Override
    public BulkRequestBuilder prepareBulk() {
        return null;
    }

    @Override
    public ActionFuture<GetResponse> get(GetRequest getRequest) {
        return null;
    }

    @Override
    public void get(GetRequest getRequest, ActionListener<GetResponse> actionListener) {

    }

    @Override
    public GetRequestBuilder prepareGet() {
        return null;
    }

    @Override
    public GetRequestBuilder prepareGet(String s, String s1, String s2) {
        return null;
    }

    @Override
    public ActionFuture<MultiGetResponse> multiGet(MultiGetRequest multiGetRequest) {
        return null;
    }

    @Override
    public void multiGet(MultiGetRequest multiGetRequest, ActionListener<MultiGetResponse> actionListener) {

    }

    @Override
    public MultiGetRequestBuilder prepareMultiGet() {
        return null;
    }

    @Override
    public ActionFuture<SearchResponse> search(SearchRequest searchRequest) {
        return null;
    }

    @Override
    public void search(SearchRequest searchRequest, ActionListener<SearchResponse> actionListener) {

    }

    @Override
    public SearchRequestBuilder prepareSearch(String... strings) {
        return null;
    }

    @Override
    public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest searchScrollRequest) {
        return null;
    }

    @Override
    public void searchScroll(SearchScrollRequest searchScrollRequest, ActionListener<SearchResponse> actionListener) {

    }

    @Override
    public SearchScrollRequestBuilder prepareSearchScroll(String s) {
        return null;
    }

    @Override
    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest multiSearchRequest) {
        return null;
    }

    @Override
    public void multiSearch(MultiSearchRequest multiSearchRequest, ActionListener<MultiSearchResponse> actionListener) {

    }

    @Override
    public MultiSearchRequestBuilder prepareMultiSearch() {
        return null;
    }

    @Override
    public ActionFuture<TermVectorsResponse> termVectors(TermVectorsRequest termVectorsRequest) {
        return null;
    }

    @Override
    public void termVectors(TermVectorsRequest termVectorsRequest, ActionListener<TermVectorsResponse> actionListener) {

    }

    @Override
    public TermVectorsRequestBuilder prepareTermVectors() {
        return null;
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVectors(String s, String s1, String s2) {
        return null;
    }

    @Override
    public ActionFuture<TermVectorsResponse> termVector(TermVectorsRequest termVectorsRequest) {
        return null;
    }

    @Override
    public void termVector(TermVectorsRequest termVectorsRequest, ActionListener<TermVectorsResponse> actionListener) {

    }

    @Override
    public TermVectorsRequestBuilder prepareTermVector() {
        return null;
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVector(String s, String s1, String s2) {
        return null;
    }

    @Override
    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(MultiTermVectorsRequest multiTermVectorsRequest) {
        return null;
    }

    @Override
    public void multiTermVectors(MultiTermVectorsRequest multiTermVectorsRequest, ActionListener<MultiTermVectorsResponse> actionListener) {

    }

    @Override
    public MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return null;
    }

    @Override
    public ExplainRequestBuilder prepareExplain(String s, String s1, String s2) {
        return null;
    }

    @Override
    public ActionFuture<ExplainResponse> explain(ExplainRequest explainRequest) {
        return null;
    }

    @Override
    public void explain(ExplainRequest explainRequest, ActionListener<ExplainResponse> actionListener) {

    }

    @Override
    public ClearScrollRequestBuilder prepareClearScroll() {
        return null;
    }

    @Override
    public ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest clearScrollRequest) {
        return null;
    }

    @Override
    public void clearScroll(ClearScrollRequest clearScrollRequest, ActionListener<ClearScrollResponse> actionListener) {

    }

    @Override
    public FieldStatsRequestBuilder prepareFieldStats() {
        return null;
    }

    @Override
    public ActionFuture<FieldStatsResponse> fieldStats(FieldStatsRequest fieldStatsRequest) {
        return null;
    }

    @Override
    public void fieldStats(FieldStatsRequest fieldStatsRequest, ActionListener<FieldStatsResponse> actionListener) {

    }

    @Override
    public FieldCapabilitiesRequestBuilder prepareFieldCaps() {
        return null;
    }

    @Override
    public ActionFuture<FieldCapabilitiesResponse> fieldCaps(FieldCapabilitiesRequest fieldCapabilitiesRequest) {
        return null;
    }

    @Override
    public void fieldCaps(FieldCapabilitiesRequest fieldCapabilitiesRequest, ActionListener<FieldCapabilitiesResponse> actionListener) {

    }

    @Override
    public Settings settings() {
        return null;
    }

    @Override
    public Client filterWithHeader(Map<String, String> map) {
        return null;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
        return null;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> actionListener) {

    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder> action) {
        return null;
    }

    @Override
    public ThreadPool threadPool() {
        return null;
    }

    @Override
    public void close() {

    }
}
