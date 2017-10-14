package com.facebook.presto.server.protocol;

import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

class PurgeQueriesRunnable
        implements Runnable
{
    private static final Logger log = Logger.get(PurgeQueriesRunnable.class);

    private final ConcurrentMap<QueryId, ActiveQuery> queries;
    private final QueryManager queryManager;

    public PurgeQueriesRunnable(ConcurrentMap<QueryId, ActiveQuery> queries, QueryManager queryManager)
    {
        this.queries = queries;
        this.queryManager = queryManager;
    }

    @Override
    public void run()
    {
        try {
            // Queries are added to the query manager before being recorded in queryIds set.
            // Therefore, we take a snapshot if queryIds before getting the live queries
            // from the query manager.  Then we remove only the queries in the snapshot and
            // not live queries set.  If we did this in the other order, a query could be
            // registered between fetching the live queries and inspecting the queryIds set.
            for (QueryId queryId : ImmutableSet.copyOf(queries.keySet())) {
                ActiveQuery query = queries.get(queryId);
                Optional<QueryState> state = queryManager.getQueryState(queryId);

                // free up resources if the query completed
                if (!state.isPresent() || state.get() == QueryState.FAILED) {
                    query.dispose();
                }

                // forget about this query if the query manager is no longer tracking it
                if (!state.isPresent()) {
                    queries.remove(queryId);
                }
            }
        }
        catch (Throwable e) {
            log.warn(e, "Error removing old queries");
        }
    }
}
