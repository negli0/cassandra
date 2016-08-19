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
import org.apache.cassandra.tools.nodetool.NetStats;
import org.apache.hadoop.mapred.join.OuterJoinRecordReader;

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
            String mode = convertData.get(NetStatsHolder.MODE) instanceof String ? (String) convertData.get(NetStatsHolder.MODE) : "";
            out.printf("Mode: %s%n", mode);

            // print StreamStatuses
            Map<Object, Object> streamStatuses = convertData.get(NetStatsHolder.STREAM_STATUSES) instanceof Map<?, ?> ? (Map) convertData.get(NetStatsHolder.STREAM_STATUSES) : Collections.emptyMap();
            if (null == streamStatuses.get(NetStatsHolder.STATUSES))
            {
                out.printf("Not sending any streams.%n");
            }
            else
            {
                Map<Object, Object> status = streamStatuses.get(NetStatsHolder.STATUSES) instanceof Map<?, ?> ? (Map) streamStatuses.get(NetStatsHolder.STATUSES) : Collections.emptyMap();
                out.printf("%s %s%n", status.get(NetStatsHolder.DESCRIPTION).toString(), status.get(NetStatsHolder.PLAN_ID));

                Map<Object, Object> session = status.get(NetStatsHolder.SESSION_INFO) instanceof Map<?, ?> ? (Map) status.get(NetStatsHolder.SESSION_INFO) : Collections.emptyMap();
                out.printf("    %s", session.get(NetStatsHolder.PEER).toString());

                Map<Object, Object> connecting = session.get(NetStatsHolder.CONNECTING) instanceof Map<?, ?> ? (Map) session.get(NetStatsHolder.CONNECTING) : Collections.emptyMap();
                if (!connecting.isEmpty())
                    out.printf(" (using) %s", connecting.get(NetStatsHolder.USING));
                out.printf("%n");

                Map<Object, Object> recvSum = session.get(NetStatsHolder.RECEIVING_SUMMARIES) instanceof Map<?, ?> ? (Map) session.get(NetStatsHolder.RECEIVING_SUMMARIES) : Collections.emptyMap();
                if (!recvSum.isEmpty())
                {
                    out.printf("        Receiving %s files, %s total. Already received %s files, %s total%n", recvSum.get(NetStatsHolder.TOTAL_FILES_TO_RECEIVE), recvSum.get(NetStatsHolder.TOTAL_SIZE_TO_RECEIVE), recvSum.get(NetStatsHolder.TOTAL_FILES_RECEIVED), recvSum.get(NetStatsHolder.TOTAL_SIZE_RECEIVED));
                    Map<Object, Object> progMap = recvSum.get(NetStatsHolder.PROGRESS) instanceof Map<?, ?> ? (Map) recvSum.get(NetStatsHolder.PROGRESS) : Collections.emptyMap();
                    for (Map.Entry<Object, Object> prog : progMap.entrySet())
                    {
                        out.printf("            %s%n", prog.getValue().toString());
                    }
                }

                Map<Object, Object> sendSum = session.get(NetStatsHolder.SENDING_SUMMARIES) instanceof Map<?, ?> ? (Map) session.get(NetStatsHolder.SENDING_SUMMARIES) : Collections.emptyMap();
                if (!sendSum.isEmpty())
                {
                    out.printf("        Sending %s files, %s total. Already sent %s files, %s total%n", sendSum.get(NetStatsHolder.TOTAL_FILES_TO_SEND), sendSum.get(NetStatsHolder.TOTAL_SIZE_TO_SEND), sendSum.get(NetStatsHolder.TOTAL_FILES_SENT), sendSum.get(NetStatsHolder.TOTAL_SIZE_SENT));
                    Map<Object, Object> progMap = sendSum.get(NetStatsHolder.PROGRESS) instanceof Map<?, ?> ? (Map) sendSum.get(NetStatsHolder.PROGRESS) : Collections.emptyMap();
                    for (Map.Entry<Object, Object> prog : progMap.entrySet())
                    {
                        out.printf("            %s%n", prog.getValue().toString());
                    }
                }
            }

            // print Read repair statistics
            Map<Object, Object> readRepairStatictics = convertData.get(NetStatsHolder.READ_REPAIR_STATICTICS) instanceof Map<?, ?> ? (Map) convertData.get(NetStatsHolder.READ_REPAIR_STATICTICS) : Collections.emptyMap();
            out.printf("Read Repair Statictics:%n");
            out.printf("Attempted: %s%n", readRepairStatictics.get(NetStatsHolder.ATTEMPTED).toString());
            out.printf("Mismatch (Blocking): %s%n", readRepairStatictics.get(NetStatsHolder.BLOCKING_MISMATCH).toString());
            out.printf("Mismatch (background): %s%n", readRepairStatictics.get(NetStatsHolder.BACKGROUND_MISMATCH).toString());


            // print Pool name
            out.printf("%-25s%10s%10s%15s%10s%n", "Pool Name", "Active", "Pending", "Completed", "Dropped");
            Map<Object, Object> threadPools = convertData.get(NetStatsHolder.MESSAGE_SERVICES) instanceof Map<?, ?> ? (Map) convertData.get(NetStatsHolder.MESSAGE_SERVICES) : Collections.emptyMap();
            for (Map.Entry<Object, Object> entry : threadPools.entrySet())
            {
                Map values = entry.getValue() instanceof Map<?, ?> ? (Map) entry.getValue() : Collections.emptyMap();
                out.printf("%-25s%10s%10s%15s%10s%n",
                           entry.getKey(),
                           values.get(NetStatsHolder.ACTIVE),
                           values.get(NetStatsHolder.PENDING),
                           values.get(NetStatsHolder.COMPLETED),
                           values.get(NetStatsHolder.DROPPED));
            }
        }
    }
}
