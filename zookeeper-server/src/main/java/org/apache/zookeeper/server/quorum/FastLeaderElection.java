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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */
public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */
    final static int maxNotificationInterval = 60000;

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */
    QuorumCnxManager manager;


    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     *
     * 假设 B 发送一个投票过来给 A 说我投票给了 C（leader）
     */
    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */
        public final static int CURRENTVERSION = 0x1;
        /**
         * 版本号
         */
        int version;
        /*
         * Proposed leader
         * 这个就是 C
         */
        long leader;
        /*
         * zxid of the proposed leader
         * 这个就是 C 的 zxid
         */
        long zxid;

        /*
         * Epoch
         * B 的 Epoch
         *
         */
        long electionEpoch;

        /*
         * current state of sender
         * B 的状态
         */
        QuorumPeer.ServerState state;

        /*
         * Address of sender
         * B 的sid
         */
        long sid;

        /*
         * epoch of the proposed leader
         * C 的 Epoch
         */
        long peerEpoch;

        @Override
        public String toString() {
            return Long.toHexString(version) + " (message format version), "
                    + leader + " (n.leader), 0x"
                    + Long.toHexString(zxid) + " (n.zxid), 0x"
                    + Long.toHexString(electionEpoch) + " (n.round), " + state
                    + " (n.state), " + sid + " (n.sid), 0x"
                    + Long.toHexString(peerEpoch) + " (n.peerEpoch) ";
        }
    }
    
    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch) {
        byte requestBytes[] = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send 
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        
        return requestBuffer;
    }

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
                long leader,
                long zxid,
                long electionEpoch,
                ServerState state,
                long sid,
                long peerEpoch) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
        }

        /*
         * Proposed leader in the case of notification
         */
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * Current state;
         */
        QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */
        long sid;
        
        /*
         * Leader epoch
         */
        long peerEpoch;
    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */
        class WorkerReceiver extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    // Sleeps on receive
                    try {
                        // 接收其他 zookeeper 发送过来的 message
                        Message response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        // 参数校验
                        if (response == null) continue;

                        // (不合法的投票)
                        // 当前的投票者集合不包含对方的服务器（observer）
                        if (!validVoter(response.sid)) {
                            // 如果对方的 sid 不在自己的具有投票节点的集合中（非法），则将自己的选票发送给它
                            // 获取自己的投票
                            Vote current = self.getCurrentVote();
                            // 构造 ToSend 消息
                            ToSend notmsg = new ToSend(ToSend.mType.notification, current.getId(), current.getZxid(),
                                    logicalclock.get(), self.getPeerState(), response.sid, current.getPeerEpoch());
                            // 放入 sendQueue 队列，等待发送
                            sendqueue.offer(notmsg);
                        }

                        // 投票合法
                        else {
                            // 检查向后兼容，因为不同版本的 zookeeper 的 vote 的长度不一样
                            if (response.buffer.capacity() < 28) {
                                LOG.error("Got a short response: " + response.buffer.capacity());
                                continue;
                            }
                            boolean backCompatibility = (response.buffer.capacity() == 28);
                            response.buffer.clear();

                            // 创建接收通知（合法投票）
                            Notification n = new Notification();
                            
                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (response.buffer.getInt()) {
                            case 0:
                                ackstate = QuorumPeer.ServerState.LOOKING;
                                break;
                            case 1:
                                ackstate = QuorumPeer.ServerState.FOLLOWING;
                                break;
                            case 2:
                                ackstate = QuorumPeer.ServerState.LEADING;
                                break;
                            case 3:
                                ackstate = QuorumPeer.ServerState.OBSERVING;
                                break;
                            default:
                                continue;
                            }
                            // 构造 Notification
                            n.leader = response.buffer.getLong();
                            n.zxid = response.buffer.getLong();
                            n.electionEpoch = response.buffer.getLong();
                            n.state = ackstate;
                            n.sid = response.sid;
                            // 不同版本的 peerEpoch 获取方式不同
                            if (!backCompatibility) {
                                n.peerEpoch = response.buffer.getLong();
                            } else {
                                if (LOG.isInfoEnabled()) {
                                    LOG.info("Backward compatibility mode, server id=" + n.sid);
                                }
                                n.peerEpoch = ZxidUtils.getEpochFromZxid(n.zxid);
                            }
                            // Version added in 3.4.6
                            n.version = (response.buffer.remaining() >= 4) ? response.buffer.getInt() : 0x0;

                            // 打印日志
                            if (LOG.isInfoEnabled()) {
                                printNotification(n);
                            }

                            // 自己的状态是LOOKING
                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                // recvqueue 中添加 Notification
                                recvqueue.offer(n);
                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                        // 如果对方的 Epoch 小于我们的 Epoch
                                        && (n.electionEpoch < logicalclock.get())) {
                                    // 将自己的选票发过去
                                    Vote v = getVote();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification, v.getId(), v.getZxid(),
                                            logicalclock.get(), self.getPeerState(), response.sid, v.getPeerEpoch());
                                    sendqueue.offer(notmsg);
                                }
                            }

                            // 自己的状态不是LOOKING
                            else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 *
                                 * 如果此服务器没有在 looking，但发送ack的服务器正在 looking
                                 * 则将其认为是领导者的内容发送回去
                                 */
                                Vote current = self.getCurrentVote();
                                // 对方正在 LOOKING
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    // 构造选票
                                    ToSend notmsg;
                                    if (n.version > 0x0) {
                                        // 推举自己为 leader
                                        notmsg = new ToSend(ToSend.mType.notification, current.getId(), current.getZxid(),
                                                current.getElectionEpoch(), self.getPeerState(), response.sid, current.getPeerEpoch());
                                    } else {
                                        Vote bcVote = self.getBCVote();
                                        notmsg = new ToSend(ToSend.mType.notification, bcVote.getId(), bcVote.getZxid(),
                                                bcVote.getElectionEpoch(), self.getPeerState(), response.sid, bcVote.getPeerEpoch());
                                    }
                                    // 放到 sendqueue 中去发送
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }

                    } catch (InterruptedException e) {
                        System.out.println("Interrupted Exception while waiting for new message" + e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }


        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */
        class WorkerSender extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager){
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    try {
                        // 从发送队列中获取消息，解耦
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if(m == null) continue;
                        // 执行发送的逻辑
                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            void process(ToSend m) {
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), 
                                                        m.leader,
                                                        m.zxid, 
                                                        m.electionEpoch, 
                                                        m.peerEpoch);
                // 调用 QuorumCnxManager 发送
                // m.sid 为目标 sid
                manager.toSend(m.sid, requestBuffer);
            }
        }


        WorkerSender ws;
        WorkerReceiver wr;

        /**
         * Constructor of class Messenger.
         *
         * @param manager   Connection manager
         */
        Messenger(QuorumCnxManager manager) {
            this.ws = new WorkerSender(manager);
            Thread t = new Thread(this.ws, "WorkerSender[myid=" + self.getId() + "]");
            t.setDaemon(true);
            t.start();

            /**
             * 注释: 创建WorkerReceiver选票接收器。
             * 其会不断地从QuorumCnxManager 中获取其他服务器发来的选举消息，并将其转换成一个选票
             * 然后保存到 recvqueue中，在选 票接收过程中，如果发现该外部选票的选举轮次小于当前服务器的
             * 那么忽略该外部投票，同时立即发送自己的内部投票。
             * 存在当前zookeeper中，用来专门和对方服务器发起链接请求，进行投票等操作的
             *
             */
            this.wr = new WorkerReceiver(manager);
            t = new Thread(this.wr, "WorkerReceiver[myid=" + self.getId() + "]");
            t.setDaemon(true);
            t.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt(){
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock(){
        return logicalclock.get();
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager){
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        // TODO_ MA注释:初始化发送队列(发起选举者发送的选票信息，就被封装成ToSend 放在一个同步队列里面)
        // TODO_ MA注释:选票发送队列，用于保存待发送的选票。操作sendqueue 的就是WorkerSender
        // TODO_ MA注释:这里会初始化的叫做: WorkerSender ,另外之前初始化的叫做: SendWorker
        sendqueue = new LinkedBlockingQueue<ToSend>();
        // TODO_ MA注释:初始化接收队列(发起选举者接受的投票结果信息，也被放在一个队列里面等待处理 )
        // TODO_ MA注释:选粟接收队列，用于保存接收到的外部投票。操作recvqueue就是workerReceiver
        recvqueue = new LinkedBlockingQueue<Notification>();
        //
        this.messenger = new Messenger(manager);
    }

    private void leaveInstance(Vote v) {
        if(LOG.isDebugEnabled()){
            LOG.debug("About to leave FLE instance: leader="
                + v.getId() + ", zxid=0x" +
                Long.toHexString(v.getZxid()) + ", my id=" + self.getId()
                + ", my state=" + self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager(){
        return manager;
    }

    volatile boolean stop;

    public void shutdown(){
        stop = true;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {
        for (QuorumServer server : self.getVotingView().values()) {
            // sid == 目标 sid
            long sid = server.id;
            // 封装要发送的信息
            ToSend notmsg = new ToSend(ToSend.mType.notification,
                    proposedLeader,
                    proposedZxid,
                    logicalclock.get(),
                    QuorumPeer.ServerState.LOOKING,
                    sid,
                    proposedEpoch);
            // 日志打印
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending Notification: " + proposedLeader + " (n.leader), 0x"  +
                      Long.toHexString(proposedZxid) + " (n.zxid), 0x" + Long.toHexString(logicalclock.get())  +
                      " (n.round), " + sid + " (recipient), " + self.getId() +
                      " (myid), 0x" + Long.toHexString(proposedEpoch) + " (n.peerEpoch)");
            }
            // 放到发送队列中
            // 消费是在 org.apache.zookeeper.server.quorum.FastLeaderElection.Messenger.WorkerSender.run
            sendqueue.offer(notmsg);
        }
    }

    private void printNotification(Notification n){
        LOG.info("Notification: " + n.toString()
                + self.getPeerState() + " (my state)");
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     * 这个方法就是比较的核心方法了
     *
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }
        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */
        return ((newEpoch > curEpoch) || 
                ((newEpoch == curEpoch) &&
                ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * Termination predicate. Given a set of votes, determines if
     * have sufficient to declare the end of the election round.
     *
     *  @param votes    Set of votes
     */
    protected boolean termPredicate(HashMap<Long, Vote> votes, Vote vote) {
        HashSet<Long> set = new HashSet<Long>();
        for (Map.Entry<Long,Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                set.add(entry.getKey());
            }
        }
        // 判断是否大于半数
        return self.getQuorumVerifier().containsQuorum(set);
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    protected boolean checkLeader(HashMap<Long, Vote> votes, long leader, long electionEpoch){

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if(leader != self.getId()){
            if(votes.get(leader) == null) predicate = false;
            else if(votes.get(leader).getState() != ServerState.LEADING) predicate = false;
        } else if(logicalclock.get() != electionEpoch) {
            predicate = false;
        } 

        return predicate;
    }
    
    /**
     * This predicate checks that a leader has been elected. It doesn't
     * make a lot of sense without context (check lookForLeader) and it
     * has been separated for testing purposes.
     * 
     * @param recv  map of received votes 
     * @param ooe   map containing out of election votes (LEADING or FOLLOWING)
     * @param n     Notification
     * @return          
     */
    protected boolean ooePredicate(HashMap<Long,Vote> recv, HashMap<Long,Vote> ooe, Notification n) {
        
        return (termPredicate(recv, new Vote(n.version, 
                                             n.leader,
                                             n.zxid, 
                                             n.electionEpoch, 
                                             n.peerEpoch, 
                                             n.state))
                && checkLeader(ooe, n.leader, n.electionEpoch));
        
    }

    synchronized void updateProposal(long leader, long zxid, long epoch){
        // 提出的 leader
        proposedLeader = leader;
        // leader 的 zxid
        proposedZxid = zxid;
        // leader 的 epoch
        proposedEpoch = epoch;
    }

    synchronized Vote getVote(){
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState(){
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        }
        else {
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
        	try {
        		return self.getCurrentEpoch();
        	} catch(IOException e) {
        		RuntimeException re = new RuntimeException(e.getMessage());
        		re.setStackTrace(e.getStackTrace());
        		throw re;
        	}
        else return Long.MIN_VALUE;
    }
    
    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    public Vote lookForLeader() throws InterruptedException {
        // JMX
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(
                    self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        // 如果开始选举的时间为0，则记录当前时间
        if (self.start_fle == 0) {
           self.start_fle = Time.currentElapsedTime();
        }
        // lookForLeader 开始
        try {
            // recvset 存储合法投票
            // 正常启动中，所有其他的服务节点，肯定会给我发送一个投票
            // 我呢 只会爆粗每一个服务器的最新合法有效的选票，放到 map 中
            HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();
            // 存储合法选举之外的投票结果
            HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();
            // 一次 vote 的等待时间： finalizeWait = 0.2
            int notTimeout = finalizeWait;
            synchronized(this) {
                // 更新逻辑时钟，每进行一次选举，都需要更新逻辑时钟
                // 当当前节点发送选票给其他服务器，会接收到他们的投票
                // 投票信息就会告诉我，他们是第几次选举
                logicalclock.incrementAndGet();
                // 更新选票（serverid zxid epoch）
                // 如果是 observer 则为0
                // 我发送给对方的称之为 选票
                // 对方发给我的，我称之为 投票
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }
            LOG.info("New election. My id =  " + self.getId() + ", proposed zxid=0x" + Long.toHexString(proposedZxid));
            // 把选票广播出去
            sendNotifications();
            // 循环，在此循环中，我们交换通知，直到找到领导者
            while ((self.getPeerState() == ServerState.LOOKING) && (!stop)) {
                // 从队列中删除下一个通知，在终止时间的2倍后超时
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                // 如果未收到足够的通知，则发送更多通知，否则将处理新通知
                // recvqueue 有设置超时时间，没有在指定时间之内没有收到投票（网络问题）
                if (n == null) {
                    if (manager.haveDelivered()) {
                        // 发了但是还没有选票过来，那我继续发
                        sendNotifications();
                    } else {
                        // 连接其他服务器再发送
                        manager.connectAll();
                    }
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = (tmpTimeOut < maxNotificationInterval ?  tmpTimeOut : maxNotificationInterval);
                    LOG.info("Notification time out: " + notTimeout);
                }

                // 判断投票（n.sid 是否有选举权，n.leader 是否有被选举权）
                else if (validVoter(n.sid) && validVoter(n.leader)) {
                    // 仅当投票来自投票视图中的副本时，才继续进行投票
                    switch (n.state) {
                    // 如果投票方处于 LOOKING 的话
                    case LOOKING:
                        // 如果我们轮次太低了，我们要把 recvset 清空
                        if (n.electionEpoch > logicalclock.get()) {
                            logicalclock.set(n.electionEpoch);
                            // 我们的轮次太低了，清除掉已有的投票
                            recvset.clear();
                            // 比较一下看看谁可以当 leader（是你推举的更优秀，还是我推举的更优秀）
                            // totalOrderPredicate 前3个为对方的参数 后3个为自己
                            // totalOrderPredicate 返回 true 就用对方的
                            if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                // 更新
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                            }
                            // 比较一下看看谁可以当 leader
                            else {
                                // 更新
                                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                            }
                            // 发送选票，可能是自己的，也可能是对方的
                            sendNotifications();
                        }
                        // 如果对方的轮次太低了 什么都不做
                        else if (n.electionEpoch < logicalclock.get()) {
                            break;
                        }
                        // 我们的轮次相同，开始真正的比较
                        else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                            // 更新自己的选票
                            updateProposal(n.leader, n.zxid, n.peerEpoch);
                            // 如果对方的更优秀，我还得再发送
                            sendNotifications();
                        }


                        // 然后把选票放到 recvset
                        recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                        // termPredicate 返回 true 说明 leader 出来了
                        if (termPredicate(recvset, new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch))) {
                            // 验证拟定领导是否有任何变更（这个步骤可有可无）
                            while ((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                    recvqueue.put(n);
                                    break;
                                }
                            }
                            // 如果我们没有从接收队列中读取任何新的相关消息，双重保险下的检验操作
                            if (n == null) {
                                // 修改状态为 LEADING FOLLOWING
                                self.setPeerState((proposedLeader == self.getId()) ? ServerState.LEADING: learningState());
                                Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }
                        break;
                    case OBSERVING:
                        break;
                    case FOLLOWING:
                    case LEADING:
                        // 对方已经是 LEADING/ FOLLOWING 了，但是要判断 Epoch
                        if (n.electionEpoch == logicalclock.get()) {
                            // 放到 recvset
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                            if (ooePredicate(recvset, outofelection, n)) {
                                // 将自己改成 LEADING
                                self.setPeerState((n.leader == self.getId()) ? ServerState.LEADING: learningState());
                                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                //
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }
                        // 在加入一个已建立的团队之前，确认大多数人跟随同一个领导者
                        outofelection.put(n.sid, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                        if (ooePredicate(outofelection, outofelection, n)) {
                            synchronized(this) {
                                logicalclock.set(n.electionEpoch);
                                self.setPeerState((n.leader == self.getId()) ? ServerState.LEADING: learningState());
                            }
                            Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                            leaveInstance(endVote);
                            return endVote;
                        }
                        break;

                    default:
                        LOG.warn("Notification state unrecognized: {} (n.state), {} (n.sid)", n.state, n.sid);
                        break;
                    }
                } // (validVoter(n.sid) && validVoter(n.leader))

                // 不用管
                else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            // JMX
            try {
                if(self.jmxLeaderElectionBean != null){
                    MBeanRegistry.getInstance().unregister(
                            self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}", manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid     Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        // 判断当前节点的具有投票资格的节点中是否包含有这个 sid
        return self.getVotingView().containsKey(sid);
    }
}
