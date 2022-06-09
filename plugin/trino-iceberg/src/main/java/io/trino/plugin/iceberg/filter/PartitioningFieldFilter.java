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
import io.trino.spi.predicate.Domain;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public abstract class PartitioningFieldFilter
        extends PartitionFilter
{
    private final String partitioningExpression;
    private final Domain domain;

    @JsonCreator
    public PartitioningFieldFilter(String partitioningExpression, Domain domain)
    {
        this.partitioningExpression = requireNonNull(partitioningExpression, "partitioningExpression is null");
        this.domain = requireNonNull(domain, "domain is null");
    }

    @JsonProperty
    public String getPartitioningExpression()
    {
        return partitioningExpression;
    }

    @JsonProperty
    public Domain getDomain()
    {
        return domain;
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
        PartitioningFieldFilter that = (PartitioningFieldFilter) o;
        return Objects.equals(partitioningExpression, that.partitioningExpression) &&
                Objects.equals(domain, that.domain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioningExpression, domain);
    }
}
