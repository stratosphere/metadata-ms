/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.flink.parameters;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.java.ExecutionEnvironment;

import com.beust.jcommander.Parameter;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;

/**
 * This class contains parameters that are necessary or useful for Stratosphere programs.
 * 
 * @author Sebastian Kruse
 */
public class FlinkParameters {

    @Parameter(names = { "-rex", "--remote-executor" }, description = "address of a remote job tracker (<host>:<port>) to run the program on", required = false)
    public String remoteExecutor;

    @Parameter(names = { "-jar", "--jars" }, description = "jar files that must be transferred to the cluster for execution", required = false, variableArity = true)
    public List<String> jarFiles = new ArrayList<String>();

    @Parameter(names = { "-dop", "--degree-of-parallelism" }, description = "jar files that must be transferred to the cluster for execution", required = false, variableArity = true)
    public int degreeOfParallelism = -1;

    @Parameter(names = { "--no-clean-up" }, description = "if temporary files shall not be removed after job execution")
    public boolean noCleanUp = false;

    @Parameter(names = { "--configuration" }, description = "a properties file with Flink configuration parameters")
    public String configurationPath = null;

    @Parameter(names = { "--wait" }, description = "seconds to wait after the program preparation")
    public int waitTime = 0;

    @Parameter(names = { "--flink-verbose" }, description = "print progress of Flink jobs")
    public boolean isFlinkVerbose = false;

    @Parameter(names = {"--rmi-server"}, description = "address of the driver for the RemoteCollectorOutputFormat")
    public String remoteCollectorOutputFormatHost;

    private LocalFlinkMiniCluster cluster;

    /**
     * Builds an execution environment based on the settings specified in the receiver object.
     * 
     * @return the {@link ExecutionEnvironment}
     */
    public ExecutionEnvironment createExecutionEnvironment() {
        if (this.remoteCollectorOutputFormatHost != null) {
            System.setProperty("java.rmi.server.hostname", this.remoteCollectorOutputFormatHost);
        }

        ExecutionEnvironment env;
        if (this.remoteExecutor != null) {
            if (this.remoteExecutor.equals("minicluster")) {
                if (this.degreeOfParallelism == -1) {
                    throw new IllegalStateException("Please specify the degree of parallelism to launch a minicluster.");
                }
                Configuration config = createConfiguration();
                config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER,
                        config.getInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, this.degreeOfParallelism));
                config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS,
                        config.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1));
//                config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 8);
//                config.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 1000);
//                config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY, 8 * 1024);

                config.setInteger("taskmanager.net.server.numThreads", 1);
                config.setInteger("taskmanager.net.client.numThreads", 1);

                cluster = new LocalFlinkMiniCluster(config, false);
                int jmPort = cluster.getJobManagerRPCPort();
                env = ExecutionEnvironment.createRemoteEnvironment("localhost", jmPort);

            } else if (this.remoteExecutor.equals("local")) {
                env = ExecutionEnvironment.createLocalEnvironment(createConfiguration());

            } else {
                final String[] hostAndPort = this.remoteExecutor.split(":");
                final String host = hostAndPort[0];
                final int port = Integer.parseInt(hostAndPort[1]);
                final String[] jars = new String[this.jarFiles.size()];
                this.jarFiles.toArray(jars);
                env = ExecutionEnvironment.createRemoteEnvironment(host, port, jars);
            }

        } else {
            env = ExecutionEnvironment.getExecutionEnvironment();
        }

        if (this.degreeOfParallelism > 0) {
            env.setParallelism(this.degreeOfParallelism);
        }

        if (this.isFlinkVerbose) {
            env.getConfig().enableSysoutLogging();
        } else {
            env.getConfig().disableSysoutLogging();
        }

        return env;
    }

    public Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        if (this.configurationPath != null) {
            try (FileInputStream fis = new FileInputStream(this.configurationPath)) {
                Properties properties = new Properties();
                properties.load(fis);
                Enumeration<?> propertyNames = properties.propertyNames();
                while (propertyNames.hasMoreElements()) {
                    String propertyName = (String) propertyNames.nextElement();
                    String value = properties.getProperty(propertyName);
                    configuration.setString(propertyName, value);
                }
            } catch (IOException e) {
                throw new RuntimeException("Could not load the configuration from " + this.configurationPath, e);
            }
        }

        return configuration;
    }

    public boolean shallCleanUp() {
        return !this.noCleanUp;
    }

    public void closeMiniClusterIfExists() {
        if (this.cluster != null && !this.cluster.getConfiguration().getBoolean("keep-running", false)) {
            this.cluster.shutdown();
        }
    }

}
