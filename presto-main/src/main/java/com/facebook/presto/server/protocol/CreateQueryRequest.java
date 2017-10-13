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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.google.common.base.Strings.isNullOrEmpty;

public class CreateQueryRequest
{
    private final SessionContext session;
    private final String query;

    @JsonCreator
    public CreateQueryRequest(
            @JsonProperty("session") SessionContext session,
            @JsonProperty("query") String query)
    {
        assertRequest(!isNullOrEmpty(query), "query is null or empty");
        this.query = query;
        this.session = assertNotNull(session, "session is null");
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public SessionContext getSession()
    {
        return session;
    }

    private static void assertRequest(boolean expression, String message)
    {
        if (!expression) {
            throw badRequest(message);
        }
    }

    private static <T> T assertNotNull(T reference, String message)
    {
        if (reference == null) {
            throw badRequest(message);
        }
        return reference;
    }

    private static WebApplicationException badRequest(String message)
    {
        throw new WebApplicationException(Response
                .status(Response.Status.BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(message)
                .build());
    }
}
