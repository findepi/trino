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
package io.trino.sql.planner.optimizations;

import io.trino.sql.planner.OptimizerStatsRecorder;
import io.trino.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public final class StatsRecordingPlanOptimizer
        implements PlanOptimizer
{
    private final OptimizerStatsRecorder stats;
    private final PlanOptimizer delegate;

    public StatsRecordingPlanOptimizer(OptimizerStatsRecorder stats, PlanOptimizer delegate)
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        stats.register(delegate);
    }

    @Override
    public PlanNode optimize(PlanNode plan, Context context)
    {
        PlanNode result;
        long duration;
        try {
            long start = System.nanoTime();
            result = delegate.optimize(plan, context);
            duration = System.nanoTime() - start;
            context.planOptimizersStatsCollector().recordOptimizer(delegate, duration);
        }
        catch (RuntimeException e) {
            stats.recordFailure(delegate);
            context.planOptimizersStatsCollector().recordFailure(delegate);
            throw e;
        }
        stats.record(delegate, duration);
        return result;
    }
}
