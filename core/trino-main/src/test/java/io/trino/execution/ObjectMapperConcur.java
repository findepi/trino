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
package io.trino.execution;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.JsonCodec;
import io.airlift.json.ObjectMapperProvider;
import io.trino.operator.DriverStats;
import io.trino.operator.OperatorStats;
import io.trino.operator.PipelineStats;
import io.trino.operator.TaskStats;
import io.trino.operator.TestDriverStats;
import io.trino.operator.TestOperatorStats;
import io.trino.operator.TestPipelineStats;
import io.trino.operator.TestTaskStats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class ObjectMapperConcur
{
    private ObjectMapperConcur() {}

    static String queryStatsJson = JsonCodec.jsonCodec(QueryStats.class).toJson(TestQueryStats.EXPECTED);
    static String stageStatsJson = JsonCodec.jsonCodec(StageStats.class).toJson(TestStageStats.EXPECTED);
    static String pipelineStatsJson = JsonCodec.jsonCodec(PipelineStats.class).toJson(TestPipelineStats.EXPECTED);
    static String driverStatsJson = JsonCodec.jsonCodec(DriverStats.class).toJson(TestDriverStats.EXPECTED);
    static String operatorStatsJson = JsonCodec.jsonCodec(OperatorStats.class).toJson(TestOperatorStats.EXPECTED);
    static String taskStatsJson = JsonCodec.jsonCodec(TaskStats.class).toJson(TestTaskStats.EXPECTED);

    static ExecutorService executorService = newCachedThreadPool(daemonThreadsNamed("foobar"));

    public static void main(String[] args)
    {
        int rounds = 1000;

        List<Future<Integer>> futures = new ArrayList<>();
        futures.add(executorService.submit(experiment(rounds)));
        futures.add(executorService.submit(experiment(rounds)));
        futures.add(executorService.submit(experiment(rounds)));
        futures.add(executorService.submit(experiment(rounds)));

        int proofOfWork = 0;
        for (Future<Integer> future : futures) {
            proofOfWork ^= getFutureValue(future);
        }
        System.out.println("proofOfWork = " + proofOfWork);
    }

    static Callable<Integer> experiment(int rounds)
    {
        return () -> {
            int proofOfWork = 0;
            for (int round = 0; round < rounds; round++) {
                // Equivalent of io.airlift.json.JsonCodec.OBJECT_MAPPER_SUPPLIER
                ObjectMapper mapper = new ObjectMapperProvider().get().enable(INDENT_OUTPUT);

                List<Future<?>> futures = new ArrayList<>();
                futures.add(executorService.submit(deserialize(mapper, QueryStats.class, queryStatsJson)));
                futures.add(executorService.submit(deserialize(mapper, StageStats.class, stageStatsJson)));
                futures.add(executorService.submit(deserialize(mapper, PipelineStats.class, pipelineStatsJson)));
                futures.add(executorService.submit(deserialize(mapper, DriverStats.class, driverStatsJson)));
                futures.add(executorService.submit(deserialize(mapper, OperatorStats.class, operatorStatsJson)));
                futures.add(executorService.submit(deserialize(mapper, TaskStats.class, taskStatsJson)));

                for (Future<?> future : futures) {
                    proofOfWork ^= getFutureValue(future).hashCode();
                }
            }
            return proofOfWork;
        };
    }

    static Callable<Object> deserialize(ObjectMapper mapper, Class<?> clazz, String json)
    {
        return () -> {
            Thread.yield(); // sync :)

            // JsonCodec::JsonCodec
            JavaType javaType = mapper.getTypeFactory().constructType(clazz);

            // JsonCodec::fromJson
            return mapper.readerFor(javaType).readValue(json);
        };
    }
}
