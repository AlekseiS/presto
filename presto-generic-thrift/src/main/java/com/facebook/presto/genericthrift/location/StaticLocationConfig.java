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
package com.facebook.presto.genericthrift.location;

import io.airlift.configuration.Config;

public class StaticLocationConfig
{
    private HostsList hosts = HostsList.empty();

    public HostsList getHosts()
    {
        return hosts;
    }

    @Config("static-location.hosts")
    public StaticLocationConfig setHosts(HostsList hosts)
    {
        this.hosts = hosts;
        return this;
    }
}
