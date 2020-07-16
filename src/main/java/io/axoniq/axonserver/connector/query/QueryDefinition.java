/*
 * Copyright (c) 2020. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.query;

import java.lang.reflect.Type;
import java.util.Objects;

/**
 * Definition of a query, constituting out of a {@code queryName} and {@code resultType}.
 */
public class QueryDefinition {

    private final String queryName;
    private final String resultType;

    /**
     * Construct a {@link QueryDefinition} out of the given {@code queryName} and {@code resultType}.
     *
     * @param queryName  the name of the query
     * @param resultType the {@link Type} of result
     */
    public QueryDefinition(String queryName, Type resultType) {
        this(queryName, resultType.getTypeName());
    }

    /**
     * Construct a {@link QueryDefinition} out of the given {@code queryName} and {@code resultType}.
     *
     * @param queryName  the name of the query
     * @param resultType the type of the result
     */
    public QueryDefinition(String queryName, String resultType) {
        this.queryName = queryName;
        this.resultType = resultType;
    }

    /**
     * Return the query name of this definition.
     *
     * @return the query name of this definition
     */
    public String getQueryName() {
        return queryName;
    }

    /**
     * Return the result type of this definition.
     *
     * @return the result type of this definition
     */
    public String getResultType() {
        return resultType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryDefinition that = (QueryDefinition) o;
        return queryName.equals(that.queryName) &&
                resultType.equals(that.resultType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryName, resultType);
    }

    @Override
    public String toString() {
        return queryName + " : " + resultType;
    }
}
