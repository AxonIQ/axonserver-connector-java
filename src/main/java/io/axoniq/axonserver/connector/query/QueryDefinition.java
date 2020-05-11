package io.axoniq.axonserver.connector.query;

import java.lang.reflect.Type;
import java.util.Objects;

public class QueryDefinition {

    private final String queryName;
    private final String resultType;

    public QueryDefinition(String queryName, String resultType) {
        this.queryName = queryName;
        this.resultType = resultType;
    }

    public QueryDefinition(String queryName, Type resultType) {
        this(queryName, resultType.getTypeName());
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

    public String getQueryName() {
        return queryName;
    }

    public String getResultType() {
        return resultType;
    }

    @Override
    public String toString() {
        return queryName + ": " + resultType;
    }
}
