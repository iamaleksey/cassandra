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

package org.apache.cassandra.service.replication.migration;

import java.util.HashMap;
import java.util.List;
import java.util.Collection;
import java.util.Map;

import com.google.common.util.concurrent.FutureCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairResult;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.AdvanceMutationTrackingMigration;
import org.apache.cassandra.tcm.transformations.AdvanceMutationTrackingMigrationV2;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Repair callback handler for mutation tracking migration.
 * Registered on repair coordinator to advance migration state on successful repairs.
 */
public class MutationTrackingRepairHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingRepairHandler.class);

    /*
     * A callback for the entire repair session. Won't advance migration state if
     * any of the individual repair jobs fail for whatever reason, and also will delay
     * advancing until all jobs are done. OTOH cuts the number of transformations committed
     * by a significant factor for keyspaces with many tables (there is one repair job per table).
     *
     * Having the callback fire at parent repair session level is inconsistent with how consensus
     * migration are handled, however, and I'm not fond of it (AY).
     */
    public static void onRepairSessionSuccess(List<RepairResult> jobResults)
    {
        try
        {
            onRepairSessionSuccessInternal(jobResults);
        }
        catch (Throwable t)
        {
            logger.error("Error handling repair completion for mutation tracking migration", t);
        }
    }

    private static void onRepairSessionSuccessInternal(List<RepairResult> jobResults)
    {
        if (jobResults.isEmpty())
            return;

        String keyspace = jobResults.get(0).desc.keyspace;
        TimeUUID parentSessionId = jobResults.get(0).desc.parentSessionId;
        ClusterMetadata clusterMetadata = ClusterMetadata.current();

        KeyspaceMigrationInfo migrationInfo = clusterMetadata.mutationTrackingMigrationState.getKeyspaceInfo(keyspace);
        if (migrationInfo == null)
        {
            logger.debug("Repair sessions (parent session {}) completed for {} but the keyspace is not migrating, " +
                         "not advancing mutation tracking migration",
                         parentSessionId, keyspace);
            return;
        }

        Map<TableId, NormalizedRanges<Token>> allRepairedPendingRanges = new HashMap<>();

        for (RepairResult jobResult : jobResults)
        {
            RepairJobDesc desc = jobResult.desc;
            String tableName = desc.columnFamily;
            Collection<Range<Token>> repairedRanges = desc.ranges;

            TableMetadata tableMetadata = clusterMetadata.schema.getKeyspaceMetadata(keyspace).getTableOrViewNullable(tableName);
            if (tableMetadata == null)
            {
                logger.warn("Repair session {} (parent session {}) completed for unknown table {}.{}, " +
                            "cannot advance mutation tracking migration",
                            desc.sessionId, desc.parentSessionId, keyspace, tableName);
                continue;
            }

            NormalizedRanges<Token> pendingRanges = migrationInfo.getPendingRangesForTable(tableMetadata.id);
            NormalizedRanges<Token> repairedPendingRanges = pendingRanges.intersection(NormalizedRanges.normalizedRanges(repairedRanges));

            if (repairedPendingRanges.isEmpty())
            {
                logger.info("Repair session {} (parent session {}) completed for {}.{} but none of the repaired ranges {} " +
                            "are still pending migration (pending: {}), not advancing mutation tracking migration",
                            desc.sessionId, desc.parentSessionId, keyspace, tableName, repairedRanges, pendingRanges);
                continue;
            }

            // Before the epoch check: an ineligible result carries no epoch and would look stale
            MutationTrackingMigrationRepairResult migrationRepairResult = jobResult.mutationTrackingMigrationRepairResult;
            //noinspection DataFlowIssue
            if (!migrationRepairResult.eligible)
            {
                logger.info("Repair session {} (parent session {}) completed for {}.{} but is ineligible to advance " +
                            "mutation tracking migration because {}",
                            desc.sessionId, desc.parentSessionId, keyspace, tableName, migrationRepairResult.ineligibleReason);
                continue;
            }

            // Epoch eligibility check: Only count repairs started after the migration started
            if (migrationRepairResult.minEpoch.isBefore(migrationInfo.startedAtEpoch))
            {
                logger.info("Repair session {} (parent session {}) completed for {}.{} but the repair started at epoch {}, " +
                            "before the migration started at epoch {}, not advancing mutation tracking migration",
                            desc.sessionId, desc.parentSessionId, keyspace, tableName, migrationRepairResult.minEpoch, migrationInfo.startedAtEpoch);
                continue;
            }

            allRepairedPendingRanges.put(tableMetadata.id, repairedPendingRanges);
        }

        if (allRepairedPendingRanges.isEmpty())
            return;

        ClusterMetadata committed = ClusterMetadataService.instance().commit(
            new AdvanceMutationTrackingMigrationV2(keyspace, allRepairedPendingRanges, migrationInfo.startedAtEpoch),
            ignore -> ignore,
            (code, message) ->
            {
                logger.info("Parent repair session {} did not advance mutation tracking migration of {}: {} ({})",
                            parentSessionId, keyspace, message, code);
                return null;
            }
        );

        if (committed == null)
            return;

        KeyspaceMigrationInfo advanced = committed.mutationTrackingMigrationState.getKeyspaceInfo(keyspace);

        logger.info("Parent repair session {} advanced mutation tracking migration of {} at epoch {}: " +
                    "contributed range(s) {} " +
                    "{} table(s) in the keyspace still migrating",
                    parentSessionId, keyspace, committed.epoch,
                    allRepairedPendingRanges,
                    advanced == null ? 0 : advanced.pendingRangesPerTable.size());

        if (advanced == null)
        {
            logger.info("Mutation tracking migration completed for keyspace {} at epoch {}, every table has been fully repaired; " +
                        "final contribution from parent repair session {}",
                        keyspace, committed.epoch, parentSessionId);
        }
    }

    public static final FutureCallback<RepairResult> completedRepairJobHandler =
        new FutureCallback<>()
        {
            @Override
            public void onSuccess(RepairResult repairResult)
            {
                try
                {
                    RepairJobDesc desc = repairResult.desc;
                    String keyspace = desc.keyspace;
                    String tableName = desc.columnFamily;
                    Collection<Range<Token>> repairedRanges = desc.ranges;

                    ClusterMetadata clusterMetadata = ClusterMetadata.current();

                    // Check if keyspace is migrating
                    KeyspaceMigrationInfo migrationInfo = clusterMetadata.mutationTrackingMigrationState.getKeyspaceInfo(keyspace);

                    if (migrationInfo == null)
                    {
                        logger.debug("Repair session {} (parent session {}) completed for {}.{} but the keyspace is not migrating, not advancing mutation tracking migration",
                                     desc.sessionId, desc.parentSessionId, keyspace, tableName);
                        return;
                    }

                    // Get table metadata
                    TableMetadata tableMetadata = clusterMetadata.schema.getKeyspaceMetadata(keyspace).getTableOrViewNullable(tableName);

                    if (tableMetadata == null)
                    {
                        logger.warn("Repair session {} (parent session {}) completed for unknown table {}.{}, cannot advance mutation tracking migration",
                                    desc.sessionId, desc.parentSessionId, keyspace, tableName);
                        return;
                    }

                    NormalizedRanges<Token> pendingRanges = migrationInfo.getPendingRangesForTable(tableMetadata.id);
                    NormalizedRanges<Token> repairedPendingRanges = pendingRanges.intersection(NormalizedRanges.normalizedRanges(repairedRanges));

                    if (repairedPendingRanges.isEmpty())
                    {
                        logger.info("Repair session {} (parent session {}) completed for {}.{} but none of the repaired ranges {} are still pending migration (pending: {}), not advancing mutation tracking migration",
                                    desc.sessionId, desc.parentSessionId, keyspace, tableName, repairedRanges, pendingRanges);
                        return;
                    }

                    MutationTrackingMigrationRepairResult migrationRepairResult = repairResult.mutationTrackingMigrationRepairResult;

                    // Before the epoch check: an ineligible result carries no epoch and would look stale
                    //noinspection DataFlowIssue
                    if (!migrationRepairResult.eligible)
                    {
                        logger.info("Repair session {} (parent session {}) completed for {}.{} but is ineligible to advance mutation tracking migration because {}",
                                    desc.sessionId, desc.parentSessionId, keyspace, tableName, migrationRepairResult.ineligibleReason);
                        return;
                    }

                    // Epoch eligibility check: Only count repairs started after the migration started
                    if (migrationRepairResult.minEpoch.isBefore(migrationInfo.startedAtEpoch))
                    {
                        logger.info("Repair session {} (parent session {}) completed for {}.{} but the repair started at epoch {}, before the migration started at epoch {}, not advancing mutation tracking migration",
                                    desc.sessionId, desc.parentSessionId, keyspace, tableName, migrationRepairResult.minEpoch, migrationInfo.startedAtEpoch);
                        return;
                    }

                    ClusterMetadata committed = ClusterMetadataService.instance().commit(
                        new AdvanceMutationTrackingMigration(keyspace, tableMetadata.id, repairedPendingRanges),
                        ignore -> ignore,
                        (code, message) ->
                        {
                            logger.info("Repair session {} (parent session {}) did not advance mutation tracking migration of {}.{}: {} ({})",
                                        desc.sessionId, desc.parentSessionId, keyspace, tableName, message, code);
                            return null;
                        });

                    if (committed == null)
                        return;

                    // Report from the metadata commit returned, not current(), which races with other epochs
                    KeyspaceMigrationInfo advanced = committed.mutationTrackingMigrationState.getKeyspaceInfo(keyspace);
                    boolean keyspaceComplete = advanced == null;
                    NormalizedRanges<Token> pending = keyspaceComplete ? NormalizedRanges.empty()
                                                                      : advanced.getPendingRangesForTable(tableMetadata.id);
                    NormalizedRanges<Token> repaired = keyspaceComplete ? KeyspaceMigrationInfo.fullRing()
                                                                       : advanced.getMigratedRangesForTable(tableMetadata.id);

                    // INFO once per repair job, with the ranges listed in full rather than a prefix
                    logger.info("Repair session {} (parent session {}) advanced mutation tracking migration of {}.{} at epoch {}: " +
                                "contributed {} range(s) {}; {} range(s) remain to be repaired {}; {} range(s) already repaired {}; " +
                                "{} table(s) in the keyspace still migrating",
                                desc.sessionId, desc.parentSessionId, keyspace, tableName, committed.epoch,
                                repairedPendingRanges.size(), repairedPendingRanges,
                                pending.size(), pending,
                                repaired.size(), repaired,
                                keyspaceComplete ? 0 : advanced.pendingRangesPerTable.size());

                    // Only the advancement that empties the last table sees the keyspace disappear
                    if (keyspaceComplete)
                        logger.info("Mutation tracking migration completed for keyspace {} at epoch {}, every table has been fully repaired; final contribution from repair session {} (parent session {}) on table {}",
                                    keyspace, committed.epoch, desc.sessionId, desc.parentSessionId, tableName);
                }
                catch (Exception e)
                {
                    logger.error("Error handling repair completion for mutation tracking migration", e);
                }
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                // noop
            }
        };
}
