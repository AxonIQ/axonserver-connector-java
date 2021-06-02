/*
 * Copyright (c) 2020-2021. AxonIQ
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

package io.axoniq.axonserver.connector.event;

import java.util.List;

/**
 * Represents a single result from an Event Query.
 * <p>
 * Entries may represent an update to previous entries. The {@link #getIdentifiers()} result will be equal to the entry
 * that is an update for. The number of entries will be the same for all results of the same query
 * <p>
 * If entries represent an ordered result, the {@link #getSortValues()} will return a list of values to sort on. The
 * number of values will be equal in all entries for the same query.
 */
public interface EventQueryResultEntry {

    /**
     * The list of columns returned by the query. Will return the same value for all result entries of the same
     * query.
     *
     * @return the names of the columns returned in this entry
     */
    List<String> columns();

    /**
     * Returns the value for the given {@code column}, casting it to the desired type.
     * <p>
     * The returned value can be a {@link String}, {@link Double}, {@link Long} or {@link Boolean}
     *
     * @param column The name of the column to return the data for
     * @param <R>    the type to cast the result to
     *
     * @return the value associated with the given column
     */
    <R> R getValue(String column);

    /**
     * Returns the identifiers that uniquely identify this result entry. When multiple entries have equal identifier,
     * the second result entry represents an updated version of the first.
     *
     * @return the identifiers for this entry
     */
    List<Object> getIdentifiers();

    /**
     * Returns the list of values to use to compare the order of this entry to other entries. Two
     * entries with the same sort values cannot be considered equal, but rather don't have a defined order between them.
     *
     * @return the values to compare the order of entries
     */
    List<Object> getSortValues();

    /**
     * Returns the String value associated with the given column
     *
     * @param column The name of the column to return the value for
     *
     * @return the value associated with the column
     * @throws ClassCastException when the value associated with the column is not a {@code String}
     */
    default String getValueAsString(String column) {
        return getValue(column);
    }

    /**
     * Returns the {@code long} value associated with the given column
     *
     * @param column The name of the column to return the value for
     *
     * @return the value associated with the column
     * @throws ClassCastException when the value associated with the column is not a {@code long}
     */
    default long getValueAsLong(String column) {
        return getValue(column);
    }

    /**
     * Returns the {@code double} value associated with the given column
     *
     * @param column The name of the column to return the value for
     *
     * @return the value associated with the column
     * @throws ClassCastException when the value associated with the column is not a {@code double}
     */
    default double getValueAsDouble(String column) {
        return getValue(column);
    }

    /**
     * Returns the {@code boolean} value associated with the given column
     *
     * @param column The name of the column to return the value for
     *
     * @return the value associated with the column
     * @throws ClassCastException when the value associated with the column is not a {@code boolean}
     */
    default boolean getValueAsBoolean(String column) {
        return getValue(column);
    }

}
