/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server.protocol;

import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Path("/v2/statement")
public class StatementResourceV2
{
    private final StatementResourceHelper statementResourceHelper;

    @Inject
    public StatementResourceV2(StatementResourceHelper statementResourceHelper)
    {
        this.statementResourceHelper = requireNonNull(statementResourceHelper, "statementResourceHelper is null");
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void createQuery(
            CreateQueryInfo queryInfo,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
            throws InterruptedException
    {
        if (queryInfo == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity(ImmutableMap.of("error", "Cannot parse create query request"))
                    .build());
        }
        SessionContext sessionContext = new JsonBasedSessionContext(queryInfo, Optional.ofNullable(servletRequest.getUserPrincipal()));
        statementResourceHelper.createQuery(queryInfo.getStatement(), sessionContext, uriInfo, asyncResponse);
    }

    @GET
    @Path("{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getQueryResults(
            @PathParam("queryId") QueryId queryId,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
            throws InterruptedException
    {
        statementResourceHelper.asyncQueryResults(queryId, token, maxWait, uriInfo, asyncResponse);
    }

    @DELETE
    @Path("{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelQuery(@PathParam("queryId") QueryId queryId,
            @PathParam("token") long token)
    {
        return statementResourceHelper.cancelQuery(queryId);
    }
}
