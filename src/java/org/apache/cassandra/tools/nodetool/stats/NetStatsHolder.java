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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

    public NetStatsHolder(NodeProbe probe, boolean humanReadable)
    {
        this.probe = probe;
        this.humanReadable = humanReadable;
    }

    @Override
    public Map<String, Object> convert2Map()
    {
        HashMap<String, Object> result = new HashMap<>();
        HashMap<String, Map<String, Object>> messageServices = new HashMap<>();
        HashMap<String, Object> Statuses = new HashMap<>();
        HashMap<String, Object> connecting = new HashMap<>();
        HashMap<String, Object> receivingSummaries = new HashMap<>();
        HashMap<String, Object> sendingSummaries = new HashMap<>();
        HashMap<String, Object> sessionInfo = new HashMap<>();
        HashMap<String, Object> streamStatuses = new HashMap<>();

        result.put("Mode", probe.getOperationMode());

        Set<StreamState> statuses = probe.getStreamStatus();
        if (statuses.isEmpty())
        {
            streamStatuses.put("Statuses", null);
        }
        else
        {
            for (StreamState status : statuses)
            {
                for (SessionInfo info : status.sessions)
                {
                    sessionInfo.put("Peer", info.peer.toString());
                    // print private IP when it is used
                    if (!info.peer.equals(info.connecting))
                    {
                        connecting.put("Using", info.connecting);
                    }
                    sessionInfo.put("Connecting", connecting);

                    System.out.printf("%n");
                    if (!info.receivingSummaries.isEmpty())
                    {
                        if (humanReadable)
                        {
                            receivingSummaries.put("TotalFilesToReceive", info.getTotalFilesToReceive());
                            receivingSummaries.put("TotalSizeToReceive", FileUtils.stringifyFileSize(info.getTotalSizeToReceive()));
                            receivingSummaries.put("TotalFilesReceived", info.getTotalFilesReceived());
                            receivingSummaries.put("TotalSizeReceived", FileUtils.stringifyFileSize(info.getTotalSizeReceived()));

                        }
                        else
                        {
                            receivingSummaries.put("TotalFilesToReceive", info.getTotalFilesToReceive());
                            receivingSummaries.put("TotalSizeToReceive", info.getTotalSizeToReceive());
                            receivingSummaries.put("TotalFilesReceived", info.getTotalFilesReceived());
                            receivingSummaries.put("TotalSizeReceived", info.getTotalSizeReceived());

                        }

                        for (ProgressInfo progress : info.getReceivingFiles())
                        {
                            receivingSummaries.put("Progress", progress.toString());
                        }
                    }
                    sessionInfo.put("ReceivingSummaries", receivingSummaries);

                    if (!info.sendingSummaries.isEmpty())
                    {
                        if (humanReadable)
                        {
                            sendingSummaries.put("TotalFilesToSend", info.getTotalFilesToSend());
                            sendingSummaries.put("TotalFileSizeToSend", FileUtils.stringifyFileSize(info.getTotalSizeToSend()));
                            sendingSummaries.put("TotalFilesSent", info.getTotalFilesSent());
                            sendingSummaries.put("TotalFileSizeSent", FileUtils.stringifyFileSize(info.getTotalSizeSent()));
                        }
                        else
                        {
                            sendingSummaries.put("TotalFilesToSend", info.getTotalFilesToSend());
                            sendingSummaries.put("TotalSizeToSend", info.getTotalSizeToSend());
                            sendingSummaries.put("TotalFilesSent", info.getTotalFilesSent());
                            sendingSummaries.put("TotalSizeSent", info.getTotalSizeSent());
                        }

                        Integer progressId = 0;
                        HashMap<Object, Object> prog = new HashMap<>();
                        for (ProgressInfo progress : info.getSendingFiles())
                        {
                            prog.put(progressId++, progress.toString());
                        }
                        sendingSummaries.put("Progress", prog);
                    }
                    sessionInfo.put("SendingSummaries", sendingSummaries);
                }
                Statuses.put("PlanID", status.planId);
                Statuses.put("Description", status.description);
                Statuses.put("SessionInfo", sessionInfo);
            }
            streamStatuses.put("Statuses", Statuses);
        }
        result.put("StreamStatuses", streamStatuses);

        HashMap<String, Object> readRepairStatistics = new HashMap<>();
        readRepairStatistics.put("Attempted", probe.getReadRepairAttempted());
        readRepairStatistics.put("BlockingMismatch", probe.getReadRepairRepairedBlocking());
        readRepairStatistics.put("BackgroundMismatch", probe.getReadRepairRepairedBackground());
        result.put("ReadRepairStatistics", readRepairStatistics);


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
        HashMap<String, Object> largeMessageService = new HashMap<>();
        largeMessageService.put("Active", "n/a");
        largeMessageService.put("Pending", pending);
        largeMessageService.put("Completed", completed);
        largeMessageService.put("Dropped", dropped);
        messageServices.put("LargeMessages", largeMessageService);

        pending = 0;
        for (int n : ms.getSmallMessagePendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getSmallMessageCompletedTasks().values())
            completed += n;
        dropped = 0;
        for (long n : ms.getSmallMessageDroppedTasks().values())
            dropped += n;
        HashMap<String, Object> smallMessageService = new HashMap<>();
        smallMessageService.put("Active", "n/a");
        smallMessageService.put("Pending", pending);
        smallMessageService.put("Completed", completed);
        smallMessageService.put("Dropped", dropped);
        messageServices.put("SmallMessages", smallMessageService);

        pending = 0;
        for (int n : ms.getGossipMessagePendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getGossipMessageCompletedTasks().values())
            completed += n;
        dropped = 0;
        for (long n : ms.getGossipMessageDroppedTasks().values())
            dropped += n;
        HashMap<String, Object> gossipMessageService = new HashMap<>();
        gossipMessageService.put("Active", "n/a");
        gossipMessageService.put("Pending", pending);
        gossipMessageService.put("Completed", completed);
        gossipMessageService.put("Dropped", dropped);
        messageServices.put("GossipMessages", gossipMessageService);

        result.put("MessageService", messageServices);

        return result;
    }
}
