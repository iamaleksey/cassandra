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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemView2;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

final class SettingsTable extends SystemView2
{
    private static final Logger logger = LoggerFactory.getLogger(SettingsTable.class);

    private static final String SETTING = "setting";
    private static final String WRITABLE = "writable";
    private static final String VALUE = "value";

    private static final Field[] FIELDS = Config.class.getFields();

    private final static Map<String, Consumer<String>> WRITABLES = ImmutableMap.<String, Consumer<String>>builder()
      .put("batch_size_warn_threshold_in_kb", v -> DatabaseDescriptor.setBatchSizeWarnThresholdInKB(Integer.parseInt(v)))
      .put("batch_size_fail_threshold_in_kb", v -> DatabaseDescriptor.setBatchSizeFailThresholdInKB(Integer.parseInt(v)))

      .put("compaction_throughput_mb_per_sec", v -> StorageService.instance.setCompactionThroughputMbPerSec(Integer.parseInt(v)))
      .put("concurrent_compactors", v -> StorageService.instance.setConcurrentCompactors(Integer.parseInt(v)))
      .put("concurrent_validations", v -> StorageService.instance.setConcurrentValidators(Integer.parseInt(v)))

      .put("tombstone_warn_threshold", v -> DatabaseDescriptor.setTombstoneWarnThreshold(Integer.parseInt(v)))
      .put("tombstone_failure_threshold", v -> DatabaseDescriptor.setTombstoneFailureThreshold(Integer.parseInt(v)))

      .put("hinted_handoff_enabled", v -> StorageProxy.instance.setHintedHandoffEnabled(Boolean.parseBoolean(v)))
      .put("hinted_handoff_throttle_in_kb", v -> StorageService.instance.setHintedHandoffThrottleInKB(Integer.parseInt(v)))

      .put("incremental_backups", v -> DatabaseDescriptor.setIncrementalBackupsEnabled(Boolean.parseBoolean(v)))

      .put("inter_dc_stream_throughput_outbound_megabits_per_sec", v -> StorageService.instance.setInterDCStreamThroughputMbPerSec(Integer.parseInt(v)))
      .put("stream_throughput_outbound_megabits_per_sec", v -> StorageService.instance.setStreamThroughputMbPerSec(Integer.parseInt(v)))

      .put("truncate_request_timeout_in_ms", v -> StorageService.instance.setTruncateRpcTimeout(Long.parseLong(v)))
      .put("cas_contention_timeout_in_ms", v -> StorageService.instance.setCasContentionTimeout(Long.parseLong(v)))
      .put("counter_write_request_timeout_in_ms", v -> StorageService.instance.setCounterWriteRpcTimeout(Long.parseLong(v)))
      .put("write_request_timeout_in_ms", v -> StorageService.instance.setWriteRpcTimeout(Long.parseLong(v)))
      .put("range_request_timeout_in_ms", v -> StorageService.instance.setRangeRpcTimeout(Long.parseLong(v)))
      .put("read_request_timeout_in_ms", v -> StorageService.instance.setReadRpcTimeout(Long.parseLong(v)))
      .put("request_timeout_in_ms", v -> StorageService.instance.setRpcTimeout(Long.parseLong(v)))

      .put("phi_convict_threshold", v -> DatabaseDescriptor.setPhiConvictThreshold(Double.parseDouble(v)))
      .build();

    static
    {
        Arrays.sort(FIELDS, Comparator.comparing(Field::getName));
    }

    private final ColumnMetadata valueColumn;

    SettingsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "settings")
                           .comment("current configuration settings")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(SETTING, UTF8Type.instance)
                           .addRegularColumn(VALUE, UTF8Type.instance)
                           .addRegularColumn(WRITABLE, BooleanType.instance)
                           .build());
        valueColumn = metadata.getColumn(ColumnIdentifier.getInterned(VALUE, false));
    }

    public boolean writable()
    {
        return true;
    }

    /**
     * Execute an update operation.
     *
     * @param partitionKey partition key for the update.
     */
    @Override
    public void mutate(DecoratedKey partitionKey, Row row) throws CassandraException
    {
        String setting = metadata.partitionKeyType.getString(partitionKey.getKey());
        if (WRITABLES.get(setting) == null)
            throw new InvalidRequestException(setting + " is not a writable setting.");
        if (row.getCell(valueColumn) == null)
            throw new InvalidRequestException("Only 'value' is updatable.");

        String value = valueColumn.type.getString(row.getCell(valueColumn).value());
        WRITABLES.get(setting).accept(value);
    }

    public void read(StatementRestrictions restrictions, QueryOptions options, ResultBuilder result)
    {
        Config config = DatabaseDescriptor.getRawConfig();
        for (Field f : FIELDS)
        {
            if (!Modifier.isStatic(f.getModifiers()))
            {
                try
                {
                    Object value = f.get(config);
                    if (value != null && value.getClass().isArray())
                    {
                        StringBuilder s = new StringBuilder("[");
                        for (int i = 0; i < Array.getLength(value); i++)
                        {
                            s.append("'");
                            s.append(Array.get(value, i));
                            s.append("'");
                            if (i < Array.getLength(value) - 1)
                                s.append(", ");
                        }
                        value = s.append("]").toString();
                    }
                    result.row(f.getName())
                            .column(VALUE, value == null ? "--" : value.toString())
                            .column(WRITABLE, WRITABLES.get(f.getName()) != null)
                            .endRow();
                }
                catch (IllegalAccessException | IllegalArgumentException e)
                {
                    logger.error("", e);
                }
            }
        }
    }
}
