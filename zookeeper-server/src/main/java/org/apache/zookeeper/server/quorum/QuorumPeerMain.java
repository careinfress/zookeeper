/**
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
package org.apache.zookeeper.server.quorum;

import java.io.File;
import java.io.IOException;

import javax.management.JMException;
import javax.security.sasl.SaslException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * 1、服务端: QuorumPeer ===>启动了ServerCnxnFactory, 启动了NIO服务端， 监听了2181端口===> 接收到客户端的一个链接请求， 则生成- - 个ServerCnxn
 * 2、客户端: Zookeeper ===> 启动和初始化了一个ClientCnxn 对线，发送ConnectRequest 给服务端
 */

/**
 *
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in
 * docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 *
 */
@InterfaceAudience.Public
public class QuorumPeerMain {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain config file";

    protected QuorumPeer quorumPeer;

    /**
     * 根据启动脚本分析得知：这个 args = zoo.cfg 的路径
     *
     * To start the replicated server specify the configuration file name on
     * the command line.
     * @param args path to the config file
     */
    public static void main(String[] args) {

        /**
         * zookeeper 启动的大致流程：
         *
         * 1. 解析配置
         * 2. 启动定时任务
         * 3. 启动开始监听端口（NIO）
         * 4. 抽象生成一个 zookeeper 节点，然后把解析出来的各种参数给配置上，然后启动
         *
         *  QuorumPeer 就是所谓的一台服务器的抽象
         *      -- 冷数据恢复
         *      -- connect start
         *      -- startLeaderElection 为选举做准备
         *      -- QuorumPeer Thread run
         *                  -- 执行选举
         */

        QuorumPeerMain main = new QuorumPeerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            System.exit(2);
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(2);
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(1);
        }
        LOG.info("Exiting normally");
        System.exit(0);
    }

    protected void initializeAndRun(String[] args) throws ConfigException, IOException {
        // 解析 zoo.cfg 配置文件内容
        QuorumPeerConfig config = new QuorumPeerConfig();
        if (args.length == 1) {
            config.parse(args[0]);
        }

        // Start and schedule the the purge task
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
                .getDataDir(), config.getDataLogDir(), config
                .getSnapRetainCount(), config.getPurgeInterval());
        // 启动清理文件的线程 DatadirCleanupManager 用来定期清理多余的快照文件
        // 每次清理，最少会依然保存 3 个最近的快照。
        // ZK 的数据会不断的从该内存快照到磁盘，快照文件越来越多
        purgeMgr.start();

        if (args.length == 1 && config.servers.size() > 0) {
            // 集群模式
            runFromConfig(config);
        } else {
            // 单机模式
            LOG.warn("Either no config or no quorum defined in config, running "
                    + " in standalone mode");
            // there is only server in the quorum -- run as standalone
            ZooKeeperServerMain.main(args);
        }
    }

    public void runFromConfig(QuorumPeerConfig config) throws IOException {
      try {
          ManagedUtil.registerLog4jMBeans();
      } catch (JMException e) {
          LOG.warn("Unable to register log4j JMX control", e);
      }
  
      LOG.info("Starting quorum peer");
      try {
          /**
           *
           * 注释: 这个 configure 方法最重要的事情就是初始化: thread 对象
           * ServerCnxnFactory 工厂对象中，有一个实例对象非常重要: thread
           * cnxnFactory 这个实例对象有一个 thread 成员属性，这个成员属性是一个线程，他的目标对象，就是 cnxnFactory
           * 其实这个thread 的start 就是执行cnxnFactory 的run( )
           * cnxnFactory的启动在: quorumPeer.start();中实现。 cnxnFactory 是 quorumPeer 的成员变量
           * config. getClientPortAddress() = 2181
           */
          ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory(); // cnxnFactory = NIOServerCnxnFactory
          cnxnFactory.configure(config.getClientPortAddress(),
                                config.getMaxClientCnxns()); // 配置的单个客户端允许的最大连接数
          // 初始化
          quorumPeer = getQuorumPeer();
          // 下面的各种 setXXX 其实就是把 QuorumPeerConfig 中的各种参数设置到 QuorumPeer
          quorumPeer.setQuorumPeers(config.getServers());
          quorumPeer.setTxnFactory(new FileTxnSnapLog(
                  new File(config.getDataLogDir()),
                  new File(config.getDataDir())));
          quorumPeer.setElectionType(config.getElectionAlg());
          quorumPeer.setMyid(config.getServerId());
          quorumPeer.setTickTime(config.getTickTime());
          quorumPeer.setInitLimit(config.getInitLimit());
          quorumPeer.setSyncLimit(config.getSyncLimit());
          quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());
          quorumPeer.setCnxnFactory(cnxnFactory);
          quorumPeer.setQuorumVerifier(config.getQuorumVerifier());
          quorumPeer.setClientPortAddress(config.getClientPortAddress());
          quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
          quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
          quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
          quorumPeer.setLearnerType(config.getPeerType());
          quorumPeer.setSyncEnabled(config.getSyncEnabled());

          // sets quorum sasl authentication configurations
          quorumPeer.setQuorumSaslEnabled(config.quorumEnableSasl);
          if (quorumPeer.isQuorumSaslAuthEnabled()) {
              quorumPeer.setQuorumServerSaslRequired(config.quorumServerRequireSasl);
              quorumPeer.setQuorumLearnerSaslRequired(config.quorumLearnerRequireSasl);
              quorumPeer.setQuorumServicePrincipal(config.quorumServicePrincipal);
              quorumPeer.setQuorumServerLoginContext(config.quorumServerLoginContext);
              quorumPeer.setQuorumLearnerLoginContext(config.quorumLearnerLoginContext);
          }

          quorumPeer.setQuorumCnxnThreadsSize(config.quorumCnxnThreadsSize);
          quorumPeer.initialize();

          /**
           * 注释:启动一个zk节点
           * 该 start() 方法中，会做三件重要的事情，之后再跳转到 quorumPeer 的run() 方法
           * 1、loadDataBase();
           * 2、cnxnFactory.start( );
           * 3、startLeaderElection();
           * 请注意:这个方法的调用完毕之后，会跳转到 QuorumPeer 的run()方法
           */
          quorumPeer.start();
          quorumPeer.join();
      } catch (InterruptedException e) {
          // warn, but generally this is ok
          LOG.warn("Quorum Peer interrupted", e);
      }
    }

    // @VisibleForTesting
    protected QuorumPeer getQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }
}
