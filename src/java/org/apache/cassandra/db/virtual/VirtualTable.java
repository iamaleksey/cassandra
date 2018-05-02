/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.virtual;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Base requirements for a VirtualTable. This is required to provide metadata about the virtual table, such as the
 * partition and clustering keys, and provide a ReadQuery for a SelectStatement.
 */
public abstract class VirtualTable
{
    protected final TableMetadata metadata;

    public VirtualTable(TableMetadata metadata)
    {
        if (!metadata.isVirtual())
            throw new IllegalArgumentException("Cannot initialize a virtual table with non-virtual table metadata " + metadata);
        this.metadata = metadata;
    }

    /**
     * Is this table writable?
     *
     * @return True if UPDATE is supported
     */
    public boolean writable()
    {
        return false;
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    /**
     * If the table allows unrestricted queries (ie filter on clustering key with no partition). Since These tables are
     * not backed by the C* data model, this restriction isnt always necessary.
     */
    public boolean allowFiltering()
    {
        return false;
    }

    /**
     * Return some implementation of a ReadQuery for a given select statement and query options.
     * 
     * @return ReadQuery
     */
    public abstract ReadQuery getQuery(SelectStatement selectStatement, QueryOptions options, DataLimits limits, int nowInSec);

    /**
     * Execute an update operation.
     *
     * @param partitionKey
     *            partition key for the update.
     */
    public void mutate(DecoratedKey partitionKey, Row row) throws CassandraException
    {
        // this should not be called unless writable is overridden
        throw new InvalidRequestException("Not Implemented");
    }
}
