/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.codelibs.fesen.index.reindex;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.common.util.CollectionUtils.isEmpty;
import static org.codelibs.fesen.core.TimeValue.timeValueNanos;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.codelibs.fesen.ExceptionsHelper;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.bulk.BackoffPolicy;
import org.codelibs.fesen.action.search.ClearScrollRequest;
import org.codelibs.fesen.action.search.ClearScrollResponse;
import org.codelibs.fesen.action.search.SearchRequest;
import org.codelibs.fesen.action.search.SearchResponse;
import org.codelibs.fesen.action.search.SearchScrollRequest;
import org.codelibs.fesen.action.search.ShardSearchFailure;
import org.codelibs.fesen.client.Client;
import org.codelibs.fesen.client.ParentTaskAssigningClient;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.document.DocumentField;
import org.codelibs.fesen.common.util.concurrent.EsRejectedExecutionException;
import org.codelibs.fesen.common.xcontent.XContentHelper;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.core.TimeValue;
import org.codelibs.fesen.index.mapper.RoutingFieldMapper;
import org.codelibs.fesen.search.SearchHit;
import org.codelibs.fesen.threadpool.ThreadPool;

/**
 * A scrollable source of hits from a {@linkplain Client} instance.
 */
public class ClientScrollableHitSource extends ScrollableHitSource {
    private final ParentTaskAssigningClient client;
    private final SearchRequest firstSearchRequest;

    public ClientScrollableHitSource(Logger logger, BackoffPolicy backoffPolicy, ThreadPool threadPool, Runnable countSearchRetry,
            Consumer<AsyncResponse> onResponse, Consumer<Exception> fail, ParentTaskAssigningClient client,
            SearchRequest firstSearchRequest) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, onResponse, fail);
        this.client = client;
        this.firstSearchRequest = firstSearchRequest;
        firstSearchRequest.allowPartialSearchResults(false);
    }

    @Override
    public void doStart(RejectAwareActionListener<Response> searchListener) {
        if (logger.isDebugEnabled()) {
            logger.debug("executing initial scroll against {}{}",
                    isEmpty(firstSearchRequest.indices()) ? "all indices" : firstSearchRequest.indices(),
                    isEmpty(firstSearchRequest.types()) ? "" : firstSearchRequest.types());
        }
        client.search(firstSearchRequest, wrapListener(searchListener));
    }

    @Override
    protected void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        SearchScrollRequest request = new SearchScrollRequest();
        // Add the wait time into the scroll timeout so it won't timeout while we wait for throttling
        request.scrollId(scrollId).scroll(timeValueNanos(firstSearchRequest.scroll().keepAlive().nanos() + extraKeepAlive.nanos()));
        client.searchScroll(request, wrapListener(searchListener));
    }

    private ActionListener<SearchResponse> wrapListener(RejectAwareActionListener<Response> searchListener) {
        return new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                searchListener.onResponse(wrapSearchResponse(searchResponse));
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class) != null) {
                    searchListener.onRejection(e);
                } else {
                    searchListener.onFailure(e);
                }
            }
        };
    }

    @Override
    public void clearScroll(String scrollId, Runnable onCompletion) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        /*
         * Unwrap the client so we don't set our task as the parent. If we *did* set our ID then the clear scroll would be cancelled as
         * if this task is cancelled. But we want to clear the scroll regardless of whether or not the main request was cancelled.
         */
        client.unwrap().clearScroll(clearScrollRequest, new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse response) {
                logger.debug("Freed [{}] contexts", response.getNumFreed());
                onCompletion.run();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("Failed to clear scroll [{}]", scrollId), e);
                onCompletion.run();
            }
        });
    }

    @Override
    protected void cleanup(Runnable onCompletion) {
        onCompletion.run();
    }

    private Response wrapSearchResponse(SearchResponse response) {
        List<SearchFailure> failures;
        if (response.getShardFailures() == null) {
            failures = emptyList();
        } else {
            failures = new ArrayList<>(response.getShardFailures().length);
            for (ShardSearchFailure failure : response.getShardFailures()) {
                String nodeId = failure.shard() == null ? null : failure.shard().getNodeId();
                failures.add(new SearchFailure(failure.getCause(), failure.index(), failure.shardId(), nodeId));
            }
        }
        List<Hit> hits;
        if (response.getHits().getHits() == null || response.getHits().getHits().length == 0) {
            hits = emptyList();
        } else {
            hits = new ArrayList<>(response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                hits.add(new ClientHit(hit));
            }
            hits = unmodifiableList(hits);
        }
        long total = response.getHits().getTotalHits().value;
        return new Response(response.isTimedOut(), failures, total, hits, response.getScrollId());
    }

    private static class ClientHit implements Hit {
        private final SearchHit delegate;
        private final BytesReference source;

        ClientHit(SearchHit delegate) {
            this.delegate = delegate;
            source = delegate.hasSource() ? delegate.getSourceRef() : null;
        }

        @Override
        public String getIndex() {
            return delegate.getIndex();
        }

        @Override
        public String getType() {
            return delegate.getType();
        }

        @Override
        public String getId() {
            return delegate.getId();
        }

        @Override
        public BytesReference getSource() {
            return source;
        }

        @Override
        public XContentType getXContentType() {
            return XContentHelper.xContentType(source);
        }

        @Override
        public long getVersion() {
            return delegate.getVersion();
        }

        @Override
        public long getSeqNo() {
            return delegate.getSeqNo();
        }

        @Override
        public long getPrimaryTerm() {
            return delegate.getPrimaryTerm();
        }

        @Override
        public String getRouting() {
            return fieldValue(RoutingFieldMapper.NAME);
        }

        private <T> T fieldValue(String fieldName) {
            DocumentField field = delegate.field(fieldName);
            return field == null ? null : field.getValue();
        }
    }
}
