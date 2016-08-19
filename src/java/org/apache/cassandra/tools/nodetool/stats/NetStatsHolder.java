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

package org.apache.cassandra.tools.nodetool.stats;

import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;
import org.apache.commons.configuration.StrictConfigurationComparator;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tools.NodeProbe;

public class NetStatsHolder implements StatsHolder
{
    public final NodeProbe probe;
    public final boolean humanReadable;

    public static final String MODE = "Mode";
    public static final String STREAM_STATUSES = "StreamStatuses";
    public static final String READ_REPAIR_STATICTICS = "ReadRepairStatictics";
    public static final String MESSAGE_SERVICES = "MessageServices";
    public static final String STATUSES = "Statuses";
    public static final String PLAN_ID = "PlanID";
    public static final String DESCRIPTION = "Description";
    public static final String SESSION_INFO = "SessionInfo";
    public static final String PEER = "Peer";
    public static final String CONNECTING = "Connecting";
    public static final String RECEIVING_SUMMARIES = "ReceivingSummaries";
    public static final String SENDING_SUMMARIES = "SendingSummaries";
    public static final String USING = "Using";
    public static final String TOTAL_FILES_TO_RECEIVE = "TotalFilesToReceive";
    public static final String TOTAL_SIZE_TO_RECEIVE = "TotalSizeToReceive";
    public static final String TOTAL_FILES_RECEIVED = "TotalFilesReceived";
    public static final String TOTAL_SIZE_RECEIVED = "TotalSizeReceived";
    public static final String TOTAL_FILES_TO_SEND = "TotalFilesToSend";
    public static final String TOTAL_SIZE_TO_SEND = "TotalSizeToSend";
    public static final String TOTAL_FILES_SENT = "TotalFilesSent";
    public static final String TOTAL_SIZE_SENT = "TotalSizeSent";
    public static final String PROGRESS = "Progress";
    public static final String ATTEMPTED = "Attempted";
    public static final String BLOCKING_MISMATCH = "BlockingMismatch";
    public static final String BACKGROUND_MISMATCH = "BackgroundMismatch";
    public static final String LARGE_MESSAGES = "LargeMessages";
    public static final String SMALL_MESSAGES = "SmallMessages";
    public static final String GOSSIP_MESSAGES = "GossipMessages";
    public static final String ACTIVE = "Active";
    public static final String PENDING = "Pending";
    public static final String COMPLETED = "Completed";
    public static final String DROPPED = "DROPPED";

    public NetStatsHolder(NodeProbe probe, boolean humanReadable)
    {
        this.probe = probe;
        this.humanReadable = humanReadable;
    }

    @Override
    public Map<String, Object> convert2Map()
    {
        Map<String, Object> result = new LinkedHashMap<>();
        Map<String, Map<String, Object>> messageServices = new LinkedHashMap<>();
        Map<String, Object> Statuses = new LinkedHashMap<>();
        Map<String, String> connecting = new LinkedHashMap<>();
        Map<String, Object> receivingSummaries = new LinkedHashMap<>();
        Map<String, Object> sendingSummaries = new LinkedHashMap<>();
        Map<String, Object> sessionInfo = new LinkedHashMap<>();
        Map<String, Object> streamStatuses = new LinkedHashMap<>();

        result.put(NetStatsHolder.MODE, probe.getOperationMode());

        Set<StreamState> statuses = probe.getStreamStatus();
        if (statuses.isEmpty())
            streamStatuses.put(NetStatsHolder.STATUSES, null);
        else
        {
            for (StreamState status : statuses)
            {
                for (SessionInfo info : status.sessions)
                {
                    sessionInfo.put(NetStatsHolder.PEER, info.peer.toString());
                    // print private IP when it is used
                    if (!info.peer.equals(info.connecting))
                        connecting.put(NetStatsHolder.USING, info.connecting.toString());

                    sessionInfo.put(NetStatsHolder.CONNECTING, connecting);

                    if (!info.receivingSummaries.isEmpty())
                    {
                        if (humanReadable)
                        {
                            receivingSummaries.put(NetStatsHolder.TOTAL_FILES_TO_RECEIVE, info.getTotalFilesToReceive());
                            receivingSummaries.put(NetStatsHolder.TOTAL_SIZE_TO_RECEIVE, FileUtils.stringifyFileSize(info.getTotalSizeToReceive()));
                            receivingSummaries.put(NetStatsHolder.TOTAL_FILES_RECEIVED, info.getTotalFilesReceived());
                            receivingSummaries.put(NetStatsHolder.TOTAL_SIZE_RECEIVED, FileUtils.stringifyFileSize(info.getTotalSizeReceived()));
                        }
                        else
                        {
                            receivingSummaries.put(NetStatsHolder.TOTAL_FILES_TO_RECEIVE, info.getTotalFilesToReceive());
                            receivingSummaries.put(NetStatsHolder.TOTAL_SIZE_TO_RECEIVE, info.getTotalSizeToReceive());
                            receivingSummaries.put(NetStatsHolder.TOTAL_FILES_RECEIVED, info.getTotalFilesReceived());
                            receivingSummaries.put(NetStatsHolder.TOTAL_SIZE_RECEIVED, info.getTotalSizeReceived());
                        }

                        Integer progressId = 0;
                        Map<Object, Object> prog = new LinkedHashMap<>();
                        for (ProgressInfo progress : info.getReceivingFiles())
                            prog.put(progressId++, progress.toString());

                        receivingSummaries.put(NetStatsHolder.PROGRESS, prog);

                        sessionInfo.put(NetStatsHolder.RECEIVING_SUMMARIES, receivingSummaries);
                    }


                    if (!info.sendingSummaries.isEmpty())
                    {
                        if (humanReadable)
                        {
                            sendingSummaries.put(NetStatsHolder.TOTAL_FILES_TO_SEND, info.getTotalFilesToSend());
                            sendingSummaries.put(NetStatsHolder.TOTAL_SIZE_TO_SEND, FileUtils.stringifyFileSize(info.getTotalSizeToSend()));
                            sendingSummaries.put(NetStatsHolder.TOTAL_FILES_SENT, info.getTotalFilesSent());
                            sendingSummaries.put(NetStatsHolder.TOTAL_SIZE_SENT, FileUtils.stringifyFileSize(info.getTotalSizeSent()));
                        }
                        else
                        {
                            sendingSummaries.put(NetStatsHolder.TOTAL_FILES_TO_SEND, info.getTotalFilesToSend());
                            sendingSummaries.put(NetStatsHolder.TOTAL_SIZE_TO_SEND, info.getTotalSizeToSend());
                            sendingSummaries.put(NetStatsHolder.TOTAL_FILES_SENT, info.getTotalFilesSent());
                            sendingSummaries.put(NetStatsHolder.TOTAL_SIZE_SENT, info.getTotalSizeSent());
                        }

                        Integer progressId = 0;
                        Map<Object, Object> prog = new LinkedHashMap<>();
                        for (ProgressInfo progress : info.getSendingFiles())
                            prog.put(progressId++, progress.toString());

                        sendingSummaries.put(NetStatsHolder.PROGRESS, prog);

                        sessionInfo.put(NetStatsHolder.SENDING_SUMMARIES, sendingSummaries);
                    }
                }
                Statuses.put(NetStatsHolder.PLAN_ID, status.planId.toString());
                Statuses.put(NetStatsHolder.DESCRIPTION, status.description);
                Statuses.put(NetStatsHolder.SESSION_INFO, sessionInfo);
            }
            streamStatuses.put(NetStatsHolder.STATUSES, Statuses);
        }
        result.put(NetStatsHolder.STREAM_STATUSES, streamStatuses);

        Map<String, Object> readRepairStatistics = new LinkedHashMap<>();
        readRepairStatistics.put(NetStatsHolder.ATTEMPTED, probe.getReadRepairAttempted());
        readRepairStatistics.put(NetStatsHolder.BLOCKING_MISMATCH, probe.getReadRepairRepairedBlocking());
        readRepairStatistics.put(NetStatsHolder.BACKGROUND_MISMATCH, probe.getReadRepairRepairedBackground());
        result.put(NetStatsHolder.READ_REPAIR_STATICTICS, readRepairStatistics);


        MessagingServiceMBean ms = probe.msProxy;
        int pending;
        long completed;
        long dropped;

        pending = 0;
        for (int n : ms.getLargeMessagePendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getLargeMessageCompletedTasks().values())
            completed += n;
        dropped = 0;
        for (long n : ms.getLargeMessageDroppedTasks().values())
            dropped += n;
        Map<String, Object> largeMessageService = new LinkedHashMap<>();
        largeMessageService.put(NetStatsHolder.ACTIVE, "n/a");
        largeMessageService.put(NetStatsHolder.PENDING, pending);
        largeMessageService.put(NetStatsHolder.COMPLETED, completed);
        largeMessageService.put(NetStatsHolder.DROPPED, dropped);
        messageServices.put(NetStatsHolder.LARGE_MESSAGES, largeMessageService);

        pending = 0;
        for (int n : ms.getSmallMessagePendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getSmallMessageCompletedTasks().values())
            completed += n;
        dropped = 0;
        for (long n : ms.getSmallMessageDroppedTasks().values())
            dropped += n;
        Map<String, Object> smallMessageService = new LinkedHashMap<>();
        smallMessageService.put(NetStatsHolder.ACTIVE, "n/a");
        smallMessageService.put(NetStatsHolder.PENDING, pending);
        smallMessageService.put(NetStatsHolder.COMPLETED, completed);
        smallMessageService.put(NetStatsHolder.DROPPED, dropped);
        messageServices.put(NetStatsHolder.SMALL_MESSAGES, smallMessageService);

        pending = 0;
        for (int n : ms.getGossipMessagePendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getGossipMessageCompletedTasks().values())
            completed += n;
        dropped = 0;
        for (long n : ms.getGossipMessageDroppedTasks().values())
            dropped += n;
        Map<String, Object> gossipMessageService = new LinkedHashMap<>();
        gossipMessageService.put(NetStatsHolder.ACTIVE, "n/a");
        gossipMessageService.put(NetStatsHolder.PENDING, pending);
        gossipMessageService.put(NetStatsHolder.COMPLETED, completed);
        gossipMessageService.put(NetStatsHolder.DROPPED, dropped);
        messageServices.put(NetStatsHolder.GOSSIP_MESSAGES, gossipMessageService);

        result.put(NetStatsHolder.MESSAGE_SERVICES, messageServices);

        return result;
    }
}
