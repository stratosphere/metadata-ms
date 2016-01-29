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
package de.hpi.isg.mdms.hadoop.cassandra;

/**
 * Includes patch from:
 * https://issues.apache.org/jira/browse/CASSANDRA-8924
 */

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractBulkRecordWriter<K, V>
        extends org.apache.hadoop.mapreduce.RecordWriter<K, V> {

    public final static String OUTPUT_LOCATION = "mapreduce.output.bulkoutputformat.localdir";
    public final static String BUFFER_SIZE_IN_MB = "mapreduce.output.bulkoutputformat.buffersize";
    public final static String STREAM_THROTTLE_MBITS = "mapreduce.output.bulkoutputformat.streamthrottlembits";
    public final static String MAX_FAILED_HOSTS = "mapreduce.output.bulkoutputformat.maxfailedhosts";

    private final Logger logger = LoggerFactory.getLogger(AbstractBulkRecordWriter.class);

    protected final Configuration conf;
    protected final int maxFailures;
    protected final int bufferSize;
    protected Closeable writer;
    protected SSTableLoader loader;
    protected Progressable progress;
    protected TaskAttemptContext context;

    protected AbstractBulkRecordWriter(TaskAttemptContext context) {
        this(HadoopCompat.getConfiguration(context));
        this.context = context;
    }

    protected AbstractBulkRecordWriter(Configuration conf, Progressable progress) {
        this(conf);
        this.progress = progress;
    }

    protected AbstractBulkRecordWriter(Configuration conf) {
        Config.setClientMode(true);
        Config.setOutboundBindAny(true);
        this.conf = conf;
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(Integer.parseInt(conf.get(STREAM_THROTTLE_MBITS, "0")));
        maxFailures = Integer.parseInt(conf.get(MAX_FAILED_HOSTS, "0"));
        bufferSize = Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64"));
    }

    protected String getOutputLocation() throws IOException {
        String dir = conf.get(OUTPUT_LOCATION, System.getProperty("java.io.tmpdir"));
        if (dir == null)
            throw new IOException("Output directory not defined, if hadoop is not setting java.io.tmpdir then define " + OUTPUT_LOCATION);
        return dir;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        close();
    }

    /**
     * Fills the deprecated RecordWriter interface for streaming.
     */
    @Deprecated
    public void close(org.apache.hadoop.mapred.Reporter reporter) throws IOException {
        close();
    }

    private void close() throws IOException {
        if (writer != null) {
            writer.close();
            Future<StreamState> future = loader.stream();
            while (true) {
                try {
                    future.get(1000, TimeUnit.MILLISECONDS);
                    break;
                } catch (ExecutionException | TimeoutException te) {
                    if (null != progress)
                        progress.progress();
                    if (null != context)
                        HadoopCompat.progress(context);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
            if (loader.getFailedHosts().size() > 0) {
                if (loader.getFailedHosts().size() > maxFailures)
                    throw new IOException("Too many hosts failed: " + loader.getFailedHosts());
                else
                    logger.warn("Some hosts failed: {}", loader.getFailedHosts());
            }
        }
    }

    public static class ExternalClient extends SSTableLoader.Client {
        private final Map<String, CFMetaData> knownCfs = new HashMap<>();
        private final Configuration conf;
        private final String hostlist;
        private final int rpcPort;
        private final String username;
        private final String password;

        public ExternalClient(Configuration conf) {
            super();
            this.conf = conf;
            this.hostlist = ConfigHelper.getOutputInitialAddress(conf);
            this.rpcPort = ConfigHelper.getOutputRpcPort(conf);
            this.username = ConfigHelper.getOutputKeyspaceUserName(conf);
            this.password = ConfigHelper.getOutputKeyspacePassword(conf);
        }

        public void init(String keyspace) {
            Set<InetAddress> hosts = new HashSet<>();
            String[] nodes = hostlist.split(",");
            for (String node : nodes) {
                try {
                    hosts.add(InetAddress.getByName(node));
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
            Iterator<InetAddress> hostiter = hosts.iterator();
            while (hostiter.hasNext()) {
                try {
                    InetAddress host = hostiter.next();
                    Cassandra.Client client = ConfigHelper.createConnection(conf, host.getHostAddress(), rpcPort);

                    // log in
                    client.set_keyspace(keyspace);
                    if (username != null) {
                        Map<String, String> creds = new HashMap<>();
                        creds.put(IAuthenticator.USERNAME_KEY, username);
                        creds.put(IAuthenticator.PASSWORD_KEY, password);
                        AuthenticationRequest authRequest = new AuthenticationRequest(creds);
                        client.login(authRequest);
                    }

                    List<TokenRange> tokenRanges = client.describe_ring(keyspace);

                    setPartitioner(client.describe_partitioner());
                    Token.TokenFactory tkFactory = getPartitioner().getTokenFactory();

                    for (TokenRange tr : tokenRanges) {
                        Range<Token> range = new Range<>(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token));
                        for (String ep : tr.endpoints) {
                            addRangeForEndpoint(range, InetAddress.getByName(ep));
                        }
                    }

                    String cfQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = '%s'",
                            Keyspace.SYSTEM_KS,
                            SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF,
                            keyspace);
                    CqlResult cfRes = client.execute_cql3_query(ByteBufferUtil.bytes(cfQuery), Compression.NONE, ConsistencyLevel.ONE);


                    for (CqlRow row : cfRes.rows) {
                        String columnFamily = UTF8Type.instance.getString(row.columns.get(1).bufferForName());
                        String columnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = '%s' AND columnfamily_name = '%s'",
                                Keyspace.SYSTEM_KS,
                                SystemKeyspace.SCHEMA_COLUMNS_CF,
                                keyspace,
                                columnFamily);
                        CqlResult columnsRes = client.execute_cql3_query(ByteBufferUtil.bytes(columnsQuery), Compression.NONE, ConsistencyLevel.ONE);

                        CFMetaData metadata = CFMetaData.fromThriftCqlRow(row, columnsRes);
                        knownCfs.put(metadata.cfName, metadata);
                    }
                    break;
                } catch (Exception e) {
                    if (!hostiter.hasNext())
                        throw new RuntimeException("Could not retrieve endpoint ranges: ", e);
                }
            }
        }

        public CFMetaData getCFMetaData(String keyspace, String cfName) {
            return knownCfs.get(cfName);
        }
    }

    public static class NullOutputHandler implements OutputHandler {
        public void output(String msg) {
        }

        public void debug(String msg) {
        }

        public void warn(String msg) {
        }

        public void warn(String msg, Throwable th) {
        }
    }
}
