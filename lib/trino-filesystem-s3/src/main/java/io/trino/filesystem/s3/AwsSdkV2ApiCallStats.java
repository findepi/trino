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
package io.trino.filesystem.s3;

import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class AwsSdkV2ApiCallStats
{
    private final TimeStat latency = new TimeStat(MILLISECONDS);
    private final CounterStat calls = new CounterStat();
    private final CounterStat failures = new CounterStat();
    private final CounterStat retries = new CounterStat();
    private final CounterStat throttlingExceptions = new CounterStat();
    private final CounterStat serverErrors = new CounterStat();

    @Managed
    @Nested
    public TimeStat getLatency()
    {
        return latency;
    }

    @Managed
    @Nested
    public CounterStat getCalls()
    {
        return calls;
    }

    @Managed
    @Nested
    public CounterStat getFailures()
    {
        return failures;
    }

    @Managed
    @Nested
    public CounterStat getRetries()
    {
        return retries;
    }

    @Managed
    @Nested
    public CounterStat getThrottlingExceptions()
    {
        return throttlingExceptions;
    }

    @Managed
    @Nested
    public CounterStat getServerErrors()
    {
        return serverErrors;
    }

    public void updateLatency(Duration duration)
    {
        latency.addNanos(duration.toNanos());
    }

    public void updateCalls()
    {
        calls.update(1);
    }

    public void updateFailures()
    {
        failures.update(1);
    }

    public void updateRetries(int retryCount)
    {
        retries.update(retryCount);
    }

    public void updateThrottlingExceptions()
    {
        throttlingExceptions.update(1);
    }

    public void updateServerErrors()
    {
        serverErrors.update(1);
    }
}
