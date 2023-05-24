/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.cdc.agent;

import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import javax.annotation.Nullable;

@Slf4j
public class AgentActivator extends io.stargate.core.activator.BaseActivator {
    public AgentActivator() {
        super("agent-s4", false);
    }

    @Nullable
    @Override
    protected ServiceAndProperties createService() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                do {
                    log.info("Waiting for Cassandra to initialize...");
                    Thread.sleep(1000);
                } while (!DatabaseDescriptor.isDaemonInitialized());
                if (!DatabaseDescriptor.isCDCEnabled()) {
                    log.error("cdc_enabled=false in your cassandra configuration, CDC agent not started.");
                } else if (DatabaseDescriptor.getCDCLogLocation() == null) {
                    log.error("cdc_raw_directory=null in your cassandra configuration, CDC agent not started.");
                } else {
                    startCdcAgent("cdcWorkingDir=/stargate/stargate-persistence-cassandra-4.0/cdc");
                }
            } catch (Exception e) {
                log.error("Error starting CDC agent", e);
            }
        });
        return null;
    }

    static void startCdcAgent(String agentArgs) throws Exception {
        log.info("Starting CDC agent, cdc_raw_directory={}", DatabaseDescriptor.getCDCLogLocation());
        AgentConfig config = AgentConfig.create(AgentConfig.Platform.PULSAR, agentArgs);


        SegmentOffsetFileWriter segmentOffsetFileWriter = new SegmentOffsetFileWriter(config.cdcWorkingDir);
        segmentOffsetFileWriter.loadOffsets();

        PulsarMutationSender pulsarMutationSender = new PulsarMutationSender(config);
        CommitLogTransfer commitLogTransfer = new BlackHoleCommitLogTransfer(config);
        CommitLogReaderServiceImpl commitLogReaderService = new CommitLogReaderServiceImpl(config, pulsarMutationSender, segmentOffsetFileWriter, commitLogTransfer);
        CommitLogProcessor commitLogProcessor = new CommitLogProcessor(DatabaseDescriptor.getCDCLogLocation(), config, commitLogTransfer, segmentOffsetFileWriter, commitLogReaderService, true);

        commitLogReaderService.initialize();

        // detect commitlogs file and submit new/modified files to the commitLogReader
        ExecutorService commitLogExecutor = Executors.newSingleThreadExecutor();
        commitLogExecutor.submit(() -> {
            try {
                do {
                    // wait to initialize the hostID before starting
                    log.info("Waiting for localhost to be initialized...");
                    Thread.sleep(1000);
                } while(StorageService.instance.getLocalHostUUID() == null);
                commitLogProcessor.initialize();
                commitLogProcessor.start();
            } catch(Exception e) {
                log.error("commitLogProcessor error:", e);
            }
        });

        ExecutorService commitLogServiceExecutor = Executors.newSingleThreadExecutor();
        commitLogServiceExecutor.submit(commitLogReaderService);

        log.info("CDC agent started");
    }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.emptyList();
  }
}
