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
package io.trino.plugin.iceberg.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DisjunctsFilter
        extends PartitionFilter
{
    private final Set<PartitionFilter> disjuncts;

    @JsonCreator
    public DisjunctsFilter(Set<PartitionFilter> disjuncts)
    {
        this.disjuncts = ImmutableSet.copyOf(requireNonNull(disjuncts, "disjuncts is null"));
    }

    @JsonProperty
    public Set<PartitionFilter> getDisjuncts()
    {
        return disjuncts;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DisjunctsFilter that = (DisjunctsFilter) o;
        return Objects.equals(disjuncts, that.disjuncts);
    }

    @Override
    public int hashCode()
    {
        return disjuncts.hashCode();
    }
}
