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
package org.apache.cassandra.repair;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;

import static org.apache.cassandra.net.EmptyMessage.emptyMessage;
import static org.apache.cassandra.net.ParameterType.FAILURE_RESPONSE;
import static org.apache.cassandra.net.Verb.REPAIR_RSP;

/**
 * Handles all repair related message.
 *
 * @since 2.0
 */
public class RepairMessageVerbHandler implements IVerbHandler<RepairMessage>
{
    public static RepairMessageVerbHandler instance = new RepairMessageVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(RepairMessageVerbHandler.class);

    private boolean isIncremental(UUID sessionID)
    {
        return ActiveRepairService.instance.consistent.local.isSessionInProgress(sessionID);
    }

    private PreviewKind previewKind(UUID sessionID)
    {
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionID);
        return prs != null ? prs.previewKind : PreviewKind.NONE;
    }

    public void doVerb(final Message<RepairMessage> message)
    {
        // TODO add cancel/interrupt message
        RepairJobDesc desc = message.payload.desc;
        try
        {
            switch (message.payload.messageType)
            {
                case PREPARE_MESSAGE:
                    PrepareMessage prepareMessage = (PrepareMessage) message.payload;
                    logger.debug("Preparing, {}", prepareMessage);
                    List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>(prepareMessage.tableIds.size());
                    for (TableId tableId : prepareMessage.tableIds)
                    {
                        ColumnFamilyStore columnFamilyStore = ColumnFamilyStore.getIfExists(tableId);
                        if (columnFamilyStore == null)
                        {
                            logErrorAndSendFailureResponse(String.format("Table with id %s was dropped during prepare phase of repair",
                                                                         tableId), message);
                            return;
                        }
                        columnFamilyStores.add(columnFamilyStore);
                    }
                    ActiveRepairService.instance.registerParentRepairSession(prepareMessage.parentRepairSession,
                                                                             message.from,
                                                                             columnFamilyStores,
                                                                             prepareMessage.ranges,
                                                                             prepareMessage.isIncremental,
                                                                             prepareMessage.timestamp,
                                                                             prepareMessage.isGlobal,
                                                                             prepareMessage.previewKind);
                    MessagingService.instance().sendResponse(Message.respond(message, emptyMessage), message.from);
                    break;

                case SNAPSHOT:
                    logger.debug("Snapshotting {}", desc);
                    final ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(desc.keyspace, desc.columnFamily);
                    if (cfs == null)
                    {
                        logErrorAndSendFailureResponse(String.format("Table %s.%s was dropped during snapshot phase of repair",
                                                                     desc.keyspace, desc.columnFamily), message);
                        return;
                    }

                    ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(desc.parentSessionId);
                    TableRepairManager repairManager = cfs.getRepairManager();
                    if (prs.isGlobal)
                    {
                        repairManager.snapshot(desc.parentSessionId.toString(), prs.getRanges(), false);
                    }
                    else
                    {
                        repairManager.snapshot(desc.parentSessionId.toString(), desc.ranges, true);
                    }
                    logger.debug("Enqueuing response to snapshot request {} to {}", desc.sessionId, message.from);
                    MessagingService.instance().sendResponse(Message.respond(message, emptyMessage), message.from);
                    break;

                case VALIDATION_REQUEST:
                    ValidationRequest validationRequest = (ValidationRequest) message.payload;
                    logger.debug("Validating {}", validationRequest);
                    // trigger read-only compaction
                    ColumnFamilyStore store = ColumnFamilyStore.getIfExists(desc.keyspace, desc.columnFamily);
                    if (store == null)
                    {
                        logger.error("Table {}.{} was dropped during snapshot phase of repair", desc.keyspace, desc.columnFamily);
                        MessagingService.instance().sendOneWay(new ValidationComplete(desc).createMessage(), message.from);
                        return;
                    }

                    ActiveRepairService.instance.consistent.local.maybeSetRepairing(desc.parentSessionId);
                    Validator validator = new Validator(desc, message.from, validationRequest.nowInSec,
                                                        isIncremental(desc.parentSessionId), previewKind(desc.parentSessionId));
                    ValidationManager.instance.submitValidation(store, validator);
                    break;

                case SYNC_REQUEST:
                    // forwarded sync request
                    SyncRequest request = (SyncRequest) message.payload;
                    logger.debug("Syncing {}", request);
                    StreamingRepairTask task = new StreamingRepairTask(desc,
                                                                       request.initiator,
                                                                       request.src,
                                                                       request.dst,
                                                                       request.ranges,
                                                                       isIncremental(desc.parentSessionId) ? desc.parentSessionId : null,
                                                                       request.previewKind,
                                                                       false);
                    task.run();
                    break;

                case ASYMMETRIC_SYNC_REQUEST:
                    // forwarded sync request
                    AsymmetricSyncRequest asymmetricSyncRequest = (AsymmetricSyncRequest) message.payload;
                    logger.debug("Syncing {}", asymmetricSyncRequest);
                    StreamingRepairTask asymmetricTask = new StreamingRepairTask(desc,
                                                                                 asymmetricSyncRequest.initiator,
                                                                                 asymmetricSyncRequest.fetchingNode,
                                                                                 asymmetricSyncRequest.fetchFrom,
                                                                                 asymmetricSyncRequest.ranges,
                                                                                 isIncremental(desc.parentSessionId) ? desc.parentSessionId : null,
                                                                                 asymmetricSyncRequest.previewKind,
                                                                                 true);
                    asymmetricTask.run();
                    break;

                case CLEANUP:
                    logger.debug("cleaning up repair");
                    CleanupMessage cleanup = (CleanupMessage) message.payload;
                    ActiveRepairService.instance.removeParentRepairSession(cleanup.parentRepairSession);
                    MessagingService.instance().sendResponse(Message.respond(message, emptyMessage), message.from);
                    break;

                case CONSISTENT_REQUEST:
                    ActiveRepairService.instance.consistent.local.handlePrepareMessage(message.from, (PrepareConsistentRequest) message.payload);
                    break;

                case CONSISTENT_RESPONSE:
                    ActiveRepairService.instance.consistent.coordinated.handlePrepareResponse((PrepareConsistentResponse) message.payload);
                    break;

                case FINALIZE_PROPOSE:
                    ActiveRepairService.instance.consistent.local.handleFinalizeProposeMessage(message.from, (FinalizePropose) message.payload);
                    break;

                case FINALIZE_PROMISE:
                    ActiveRepairService.instance.consistent.coordinated.handleFinalizePromiseMessage((FinalizePromise) message.payload);
                    break;

                case FINALIZE_COMMIT:
                    ActiveRepairService.instance.consistent.local.handleFinalizeCommitMessage(message.from, (FinalizeCommit) message.payload);
                    break;

                case FAILED_SESSION:
                    FailSession failure = (FailSession) message.payload;
                    ActiveRepairService.instance.consistent.coordinated.handleFailSessionMessage(failure);
                    ActiveRepairService.instance.consistent.local.handleFailSessionMessage(message.from, failure);
                    break;

                case STATUS_REQUEST:
                    ActiveRepairService.instance.consistent.local.handleStatusRequest(message.from, (StatusRequest) message.payload);
                    break;

                case STATUS_RESPONSE:
                    ActiveRepairService.instance.consistent.local.handleStatusResponse(message.from, (StatusResponse) message.payload);
                    break;

                default:
                    ActiveRepairService.instance.handleMessage(message.from, message.payload);
                    break;
            }
        }
        catch (Exception e)
        {
            logger.error("Got error, removing parent repair session");
            if (desc != null && desc.parentSessionId != null)
                ActiveRepairService.instance.removeParentRepairSession(desc.parentSessionId);
            throw new RuntimeException(e);
        }
    }

    private void logErrorAndSendFailureResponse(String errorMessage, Message<?> respondTo)
    {
        logger.error(errorMessage);
        Message reply = Message.respondWithParameter(respondTo, emptyMessage, FAILURE_RESPONSE, MessagingService.ONE_BYTE);
        MessagingService.instance().sendResponse(reply, respondTo.from);
    }
}
