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

import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.cql3.CQL3Type;

public class NetStatsPrinter
{
    public static StatsPrinter from(String format)
    {
        switch (format)
        {
            case "json":
                return new StatsPrinter.JsonPrinter();
            case "yaml":
                return new StatsPrinter.YamlPrinter();
            default:
                return new DefaultPrinter();
        }
    }

    public static class DefaultPrinter implements StatsPrinter<NetStatsHolder>
    {
        @Override
        public void print(NetStatsHolder data, PrintStream out)
        {
            Map<String, Object> convertData = data.convert2Map();

            // print Mode
            String mode = convertData.get("Mode") instanceof String ? (String) convertData.get("Mode") : "";
            out.printf("Mode: %s%n", mode);

            // print StreamStatuses
            Map<Object, Object> streamStatuses = convertData.get("StreamStatuses") instanceof Map<?, ?> ? (Map) convertData.get("StreamStatuses") : Collections.emptyMap();
            if (null == streamStatuses.get("Statuses"))
            {
                out.printf("Not sending any streams.%n");
            }
            else
            {
                Map<Object, Object> status = streamStatuses.get("Statuses") instanceof Map<?, ?> ? (Map) streamStatuses.get("Statuses") : Collections.emptyMap();
                out.printf("%s %s%n", status.get("Description").toString(), status.get("PlanID").toString());

                Map<Object, Object> session = status.get("SessionInfo") instanceof Map<?, ?> ? (Map) status.get("SessionInfo") : Collections.emptyMap();
                out.printf("    %s", session.get("Peer").toString());

                Map connecting = session.get("Connecting") instanceof Map<?, ?> ? (Map) session.get("Connecting") : Collections.emptyMap();
                out.printf(" (using) %s", connecting.get("Using").toString());

                Map recvSum = session.get("ReceivingSummaries") instanceof Map<?, ?> ? (Map) session.get("ReceivingSummaries") : Collections.emptyMap();
                out.printf("        Receiving %s files, %s total. Already received %s files, %s total%n", recvSum.get("TotalFilesToReceive"), recvSum.get("TotalSizeToReceive"), recvSum.get("TotalFilesReceived"), recvSum.get("TotalSizeReceived"));
                Map<Object, Object> progMap = recvSum.get("Progress") instanceof Map<?, ?> ? (Map) recvSum.get("Progress") : Collections.emptyMap();
                for (Map.Entry<Object, Object> prog : progMap.entrySet())
                {
                    out.printf("            %s%n", prog.getValue().toString());
                }

                Map sendSum = session.get("ReceivingSummaries") instanceof Map<?, ?> ? (Map) session.get("ReceivingSummaries") : Collections.emptyMap();
                out.printf("        Sending %s files, %s total. Already sent %s files, %s total%n", sendSum.get("TotalFilesToSend"), sendSum.get("TotalSizeToSend"), sendSum.get("TotalFilesSent"), sendSum.get("TotalSizeSent"));
                progMap = sendSum.get("Progress") instanceof Map<?, ?> ? (Map) sendSum.get("Progress") : Collections.emptyMap();
                for (Map.Entry<Object, Object> prog : progMap.entrySet())
                {
                    out.printf("            %s%n", prog.getValue().toString());
                }
            }

            // print Read repair statistics
            Map<Object, Object> readRepairStatictics = convertData.get("ReadRepairStatistics") instanceof Map<?, ?> ? (Map) convertData.get("ReadRepairStatistics") : Collections.emptyMap();
            out.printf("Read Repair Statictics:%n");
            out.printf("Attempted: %s%n", readRepairStatictics.get("Attempted").toString());
            out.printf("Mismatch (Blocking): %s%n", readRepairStatictics.get("BlockingMismatch").toString());
            out.printf("Mismatch (background): %s%n", readRepairStatictics.get("BackgroundMismatch").toString());


            // print Pool name
            out.printf("%-25s%10s%10s%15s%10s%n", "Pool Name", "Active", "Pending", "Completed", "Dropped");
            Map<Object, Object> threadPools = convertData.get("MessageService") instanceof Map<?, ?> ? (Map) convertData.get("MessageService") : Collections.emptyMap();
            for (Map.Entry<Object, Object> entry : threadPools.entrySet())
            {
                Map values = entry.getValue() instanceof Map<?, ?> ? (Map) entry.getValue() : Collections.emptyMap();
                out.printf("%-25s%10s%10s%15s%10s%n",
                           entry.getKey(),
                           values.get("Active"),
                           values.get("Pending"),
                           values.get("Completed"),
                           values.get("Dropped"));
            }
        }
    }
}
