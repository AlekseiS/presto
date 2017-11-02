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
package com.facebook.presto.client.v2;

import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.QueryStatusInfo;
import com.google.common.net.MediaType;
import io.airlift.json.JsonCodec;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.BufferedReader;
import java.io.IOException;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;

public final class StatusInfoResponseHandler
{
    public static final JsonCodec<QueryResults> QUERY_RESULTS_JSON_CODEC = jsonCodec(QueryResults.class);

    private StatusInfoResponseHandler() {}

    public static DataResponse<QueryResults> handle(Response response)
    {
        ResponseBody responseBody = null;
        try {
            responseBody = response.body();
            if (responseBody == null) {
                throw new CommunicationException("Response body is null");
            }
            // otherwise we must have gotten an OK response, everything else is considered fatal
            if (response.code() != HTTP_OK) {
                StringBuilder body = new StringBuilder();
                // TODO: check if body is present, close response
                try (BufferedReader reader = new BufferedReader(responseBody.charStream())) {
                    // Get up to 1000 lines for debugging
                    for (int i = 0; i < 1000; i++) {
                        String line = reader.readLine();
                        // Don't output more than 100KB
                        if (line == null || body.length() + line.length() > 100 * 1024) {
                            break;
                        }
                        body.append(line);
                        body.append('\n');
                    }
                }
                catch (RuntimeException | IOException e) {
                    // Ignored. Just return whatever message we were able to decode
                }
                throw new CommunicationException(format("Expected response code to be 200, but was %s: %s", response.code(), body.toString()));
            }

            // invalid content type can happen when an error page is returned, but is unlikely given the above 200
            String contentType = response.header(CONTENT_TYPE);
            if (contentType == null) {
                throw new CommunicationException(format("%s header is not set: %s", CONTENT_TYPE, response));
            }

//            if (!mediaTypeMatches(contentType, MediaType.JSON_UTF_8)) {
//                throw new CommunicationException(format("Expected %s response from server but got %s", MediaType.JSON_UTF_8, contentType));
//            }

            try {
                byte[] responseBodyBytes = responseBody.bytes();
                QueryResults queryResults = QUERY_RESULTS_JSON_CODEC.fromJson(responseBodyBytes);
                return new DataResponse<>(queryResults, responseBodyBytes.length, queryResults.getNextUri());
            }
            catch (IOException e) {
                throw new CommunicationException("Error parsing json", e);
            }
        }
        finally {
            if (responseBody != null) {
                responseBody.close();
            }
            response.close();
        }
    }

    private static boolean mediaTypeMatches(String value, MediaType range)
    {
        try {
            return MediaType.parse(value).is(range);
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            return false;
        }
    }
}
