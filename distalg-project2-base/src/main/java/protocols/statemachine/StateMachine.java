package protocols.statemachine;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.StealLeaderRequest;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.notifications.ClientRequestReply;
import protocols.statemachine.requests.OrderRequest;
import protocols.statemachine.messages.ForwardOpMessage;
import protocols.statemachine.timers.ReconnectTimer;
import protocols.statemachine.utils.PendingOp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * This is NOT fully functional StateMachine implementation.
 * This is simply an example of things you can do, and can be used as a starting point.
 *
 * You are free to change/delete anything in this class, including its fields.
 * The only thing that you cannot change are the notifications/requests between the StateMachine and the APPLICATION
 * You can change the requests/notification between the StateMachine and AGREEMENT protocol, however make sure it is
 * coherent with the specification shown in the project description.
 *
 * Do not assume that any logic implemented here is correct, think for yourself!
 */

public class StateMachine extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    private enum State {JOINING, ACTIVE}

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StateMachine";
    public static final short PROTOCOL_ID = 200;

    private final Host self;     //My own address/port
    private final int channelId; //Id of the created channel

    private State state;
    private List<Host> membership;
    private int nextInstance; // // This *proposal* progress

    // To [Operation Ordering]
    private final Map<Integer, DecidedNotification> decidedBuffer; // When (k) gets before (k-1), (k) is stored until (k-1) is executed.
    private int nextExecuteInstance; // Incremental instance order
    private final Queue<OrderRequest> waitingLeader = new LinkedList<>(); // Requests received while leader unknow

    // To [Leader Fowarding]
    private Host currentLeader; // null if unknow
    private final Map<UUID, PendingOp> pendingOps = new HashMap<>();

    // To [Connection Management]
    private final Set<Host> connectedPeers = new HashSet<>(); // Current active TCP connections
    private final Set<Host> reconnectScheduled = new HashSet<>(); // Used to scheduling duplicate reconnect timers
    private final Map<Host, Queue<ForwardOpMessage>> outboundBuffer = new HashMap<>(); // Used when leader is unreachable
    private static final long RECONNECT_BASE_MS = 200; // Initial time to reconnect
    private static final long RECONNECT_MAX_MS = 5000; // Max time to reconnect
    private final Map<Host, Long> reconnectDelay = new HashMap<>(); // Used to get the time is getting to reconnect

    private final Set<UUID> seenForwardedOps = new HashSet<>(); // Used to ignore duplicate ForwardOpMessage replays beacuse reconnects
    private final Set<UUID> decidedOpIds = new HashSet<>(); // Used to ignore forwarded messages of operations already decided

    private static final long RECONNECT_JITTER_MAX_MS = 50; // Randomness
    private static final int MAX_BUFFER_PER_PEER = 1000; // Max size of buffer

    private final short agreementProtoId; // Genreric Protocol reciever (Raft/Multi-Paxos)

    // To Better performance - [Steal Leader]
    private static final double DECAY_FACTOR = 0.5; // who much it's gonna to reduce each time
    private static final long DECAY_INTERVAL_MS = 500; // Time to reduce the weigth
    private static final double STEAL_THRESHOLD = 300.0; // weigth need it to steal the leader
    private double effectiveForwardWeigth = 0.0;
    private long lastUpdateTimestamp = System.currentTimeMillis();

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        
        // ordering
        nextExecuteInstance = 0;
        decidedBuffer = new HashMap<>();

        // Starting the currentLeader is null until someone tells that isn't
        this.currentLeader = null;

        // Proposal instance
        this.nextInstance = 0;

        // Get agreement Protocol Id
        this.agreementProtoId = Short.parseShort(props.getProperty("agreement_proto_id", "100"));

        String address = props.getProperty("babel.address");
        String port = props.getProperty("babel.port");

        logger.info("Listening on {}:{}", address, port);
        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port); //The port to bind to
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelId = createChannel(TCPChannel.NAME, channelProps);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*-------------------- Register SMR Internal Handlers ------------------------------- */
        // used to forwarding leader from other replicas
        registerMessageSerializer(channelId, ForwardOpMessage.MSG_ID, ForwardOpMessage.serializer);
        // Used to processes forwarded operations received from other replicas
        registerMessageHandler(channelId, ForwardOpMessage.MSG_ID, this::uponForwardOpMessage, this::uponMsgFail);
        // Used to retry failed peer connections
        registerTimerHandler(ReconnectTimer.TIMER_ID, this::uponReconnectTimer);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);
        subscribeNotification(LeaderChangeNotification.NOTIFICATION_ID, this::uponLeaderChangeNotification);
    }

    @Override
    public void init(Properties props) {
        //Inform the state machine protocol about the channel we created in the constructor
        triggerNotification(new ChannelReadyNotification(channelId, self));

        String host = props.getProperty("initial_membership");
        String[] hosts = host.split(",");
        List<Host> initialMembership = new LinkedList<>();
        for (String s : hosts) {
            String[] hostElements = s.split(":");
            Host h;
            try {
                h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));
            } catch (UnknownHostException e) {
                throw new AssertionError("Error parsing initial_membership", e);
            }
            initialMembership.add(h);
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
            logger.error("Thread was interrupted during initialization", e);
        }
        if (initialMembership.contains(self)) {
            state = State.ACTIVE;
            logger.info("Starting in ACTIVE as I am part of initial membership");
            //I'm part of the initial membership, so I'm assuming the system is bootstrapping
            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);
            triggerNotification(new JoinedNotification(membership, 0));
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");
            //You have to do something to join the system and know which instance you joined
            // (and copy the state of that instance)
        }

    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        if (state == State.JOINING) {
            //Do something smart (like buffering the requests)
            return;
        } 
        
        if (currentLeader == null) {
            // Add to buffer
            waitingLeader.add(request);

            logger.debug("Buffered opId {} (reason= unknow leader, waitingLeaderSize={})",
                request.getOpId(), waitingLeader.size());
            return;
        }

        // Local op became pending
        trackPending(request.getOpId(), request.getOperation(), self);

        if (self.equals(currentLeader)) {
            // If self is leader, doesn't need to steal the leader from himself
            effectiveForwardWeigth = 0;

            // Send(ProposeReq(Instance, OpId, Op), ProtocolID)
            sendRequest(new ProposeRequest(nextInstance++, request.getOpId(), request.getOperation()),
                    agreementProtoId);
        } else {
            long now = System.currentTimeMillis();
            long timePassed = now - lastUpdateTimestamp;

            // Should I steal now the leader?

            // Check if should reduce the time
            if (timePassed >= DECAY_INTERVAL_MS) {
                // Calculate how many decay periods pass - [RDProb]
                int intervals = (int) (timePassed / DECAY_INTERVAL_MS);

                // (exponential reducer) *= intervals ^ (DECAY_FACTOR) - [RDProb]
                effectiveForwardWeigth *= Math.pow(DECAY_FACTOR, intervals);
                
                // Update time
                lastUpdateTimestamp = now;
            }

            // Inc Weigth of ops sent to leader, to steal if need to
            effectiveForwardWeigth += 1.0; // New weigth more important than old weigth

            // Check if he should steal
            if (effectiveForwardWeigth >= STEAL_THRESHOLD) {
                logger.info("Order volume (Rate: {}) is very high. Trying *kindly* taking the leader.", 
                            String.format("%.2f", effectiveForwardWeigth));
                
                // Req to Agreement to Steal Leader
                sendRequest(new StealLeaderRequest(), agreementProtoId);
                
                // Whether or not he managed to steal the lead, he's going to restart
                effectiveForwardWeigth = 0; 
            }

            // Send(FwMsg(OpId, Op), Leader)
            sendOrBufferToHost(new ForwardOpMessage(request.getOpId(), request.getOperation()), currentLeader);
            
            logger.debug("Forwarding opId {} to leader {}", request.getOpId(), currentLeader);
        }
    }

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.debug("Received notification: {}", notification);
        int instance = notification.getInstance();

        // Assuming that k is decided, the next one must be (at least) k+1
        nextInstance = Math.max(nextInstance, instance + 1);
        
        // Catch already executed instance (number less then actual instance expected)
        if (instance < nextExecuteInstance) {
            logger.debug("Ignoring because was already executed the instance {}", instance);
            return;
        }

        // No duplication
        decidedBuffer.putIfAbsent(instance, notification);

        // Mark operation as decided to block future replayed forwards
        decidedOpIds.add(notification.getOpId());

        // Decided, clear Pending
        clearPending(notification.getOpId());

        tryExecuteInOrder();
    }
    
    private void uponLeaderChangeNotification(LeaderChangeNotification notification, short sourceProto) {
        Host newLeader = notification.getLeaderID();
        
        // Ignore invalid leaders updates
        if (newLeader != null && !isKnownMember(newLeader)) {
            logger.warn("Ignoring LeaderChange to non-member host {} (membership={})", newLeader, membership);
            return;
        }

        Host oldLeader = this.currentLeader;
        this.currentLeader = newLeader;
        logger.info("Leader changed: {} -> {}", oldLeader, newLeader);
        
        if (newLeader == null) return;

        // Process the "lost" Ops while the leader was unknow
        drainWaitingLeaderQueue();

        if (pendingOps.isEmpty()) return;

        if (self.equals(newLeader)) {
            // Ask for pending Operations not made it
            for (PendingOp p : pendingOps.values()) {
                // Send(ProposeRequest(Instance, OpId, Op), ProtocolID)
                sendRequest(
                    new ProposeRequest(nextInstance++, p.getOpId(), p.getOperation()),
                    agreementProtoId);
            }
        } else {
            // Send to new leader
            for (PendingOp p : pendingOps.values()) {
                // Send(FwMsg(OpId, Op), newLeader)
                sendOrBufferToHost(
                    new ForwardOpMessage(p.getOpId(), p.getOperation()),
                    newLeader
                );
            }
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host h = event.getNode();
        
        // Mark peer connected
        connectedPeers.add(h);

        // Reset time to reconnect
        reconnectDelay.put(h, RECONNECT_BASE_MS);

        // Flush buffered forwarded operations to that peer
        Queue<ForwardOpMessage> q = outboundBuffer.get(h);
        while (q != null && !q.isEmpty()) {
            // Send(MsgOp, host)
            sendMessage(q.poll(), h);
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host h = event.getNode();

        // Remove from connecteds
        connectedPeers.remove(h);

        // Schedule reconnect attempt
        ensureReconnect(h);    
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host h = event.getNode();

        // Keep disnonnected
        connectedPeers.remove(h);
        
        // Schedule reconnect attempt
        ensureReconnect(h);
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    private void uponForwardOpMessage(ForwardOpMessage msg, Host from, short sourceProto, int channelId) {
        // Check if host isn't leader
        if (currentLeader == null || !self.equals(currentLeader)) return;

        // Only accept from knowed replicas
        if (!isKnownMember(from)) {
            logger.warn("Ignoring ForwardOpMessage from unknown host {} opId={}", from, msg.getOpId());
            return;
        }    
        
        // Get OpId
        UUID opId = msg.getOpId();

        // Already decided
        if (decidedOpIds.contains(opId)) return;

        // Already on pending
        if (!seenForwardedOps.add(opId)) return;

        // FwOp pending at leader
        trackPending(msg.getOpId(), msg.getOperation(), from);

        // Send(Propose(Instance, OpId, Op), ProtocolID)
        sendRequest(new ProposeRequest(nextInstance++, msg.getOpId(), msg.getOperation()), 
            agreementProtoId);
    }

    private void sendOrBufferToHost(ForwardOpMessage msg, Host dst) {
        // Check destination(dst)
        if (dst == null) return;
        
        if (connectedPeers.contains(dst)) {
            // Send msg to destination
            sendMessage(msg, dst);
        } else {
            Queue<ForwardOpMessage> q = outboundBuffer.computeIfAbsent(dst, h -> new LinkedList<>());
            
            // Drop the oldest of queue to keep buffer bounded
            if (q.size() >= MAX_BUFFER_PER_PEER) {
                ForwardOpMessage dropped = q.poll();
                logger.warn("Outbound buffer full for {} (max={}), dropping oldest opId={}",
                        dst, MAX_BUFFER_PER_PEER, dropped != null ? dropped.getOpId() : null);
            }

            // Add it
            q.add(msg);

            logger.debug("Buffered opId {} for {} (reason=no connection, queueSize={})",
                msg.getOpId(), dst, q.size());

            // Trigger reconnect workflow
            ensureReconnect(dst);
        }
    }

    private void ensureReconnect(Host h) {
        // Ignore invalid targets
        if (h == null || h.equals(self)) return;
        
        // Ignore host if isn't part of the network membership
        if (!isKnownMember(h)) return;

        // Avoid scheduling duplicate reconnect timers
        if (reconnectScheduled.contains(h)) return;
    
        reconnectScheduled.add(h);

        // Get delay time
        long baseDelay = reconnectDelay.getOrDefault(h, RECONNECT_BASE_MS);
        
        long jitter = ThreadLocalRandom.current().nextLong(RECONNECT_JITTER_MAX_MS + 1);
        long scheduleDelay = Math.min(baseDelay + jitter, RECONNECT_MAX_MS);

        logger.debug("Reconnect scheduled to {} in {} ms (base={} ms, jitter={} ms)",
            h, scheduleDelay, baseDelay, jitter);

        // Schedule reconnect attempt in the future
        setupTimer(new ReconnectTimer(h), scheduleDelay);
    }

    private void uponReconnectTimer(ReconnectTimer timer, long timerId) {
        Host h = timer.getTarget();
        
        // Timer failed
        reconnectScheduled.remove(h);
        
        // If it's invalid to reconnect, don't need it
        if (h == null || !isKnownMember(h) || connectedPeers.contains(h)) return;
        
        // Attempt to reconnect
        openConnection(h);
        
        // Calculate new delay: Min(oldDelay * 2, RECONNECT_MAX_MS)
        long oldDelay = reconnectDelay.getOrDefault(h, RECONNECT_BASE_MS);
        reconnectDelay.put(h, Math.min(oldDelay * 2, RECONNECT_MAX_MS));
    }

    /* --------------------------------- Auxiliar Functions ---------------------------- */
    // Return everything to [starting - nextExecuteInstance]
    private void tryExecuteInOrder() {
        DecidedNotification next;
        while ((next = decidedBuffer.remove(nextExecuteInstance)) != null) {
            logger.debug("Executing decided instance {} opId {}", nextExecuteInstance, next.getOpId());

            triggerNotification(new ClientRequestReply(next.getOpId(), next.getOperation()));
            
            // Update nextExecuteInstance
            nextExecuteInstance++;
        }
    }

    private void trackPending(UUID opId, byte[] operation, Host origin) {
        pendingOps.putIfAbsent(opId, new PendingOp(opId, operation, origin));
    }
    
    private void clearPending(UUID opId) {
        pendingOps.remove(opId);
    }

    private void drainWaitingLeaderQueue() {
        // Still having no leader
        if (currentLeader == null) return;
        
        // Send to leader, with regular flow
        OrderRequest req;
        while ((req = waitingLeader.poll()) != null) {
            // Op become pending
            trackPending(req.getOpId(), req.getOperation(), self);
    
            if (self.equals(currentLeader)) {
                // Send(ProposeReq(Instance, OpId, Op), ProtocolID)
                sendRequest(
                    new ProposeRequest(nextInstance++, req.getOpId(), req.getOperation()),
                    agreementProtoId);
            } else {
                // Send(FwMsg(OpId, Op), Leader)
                sendOrBufferToHost(
                    new ForwardOpMessage(req.getOpId(), req.getOperation()),
                    currentLeader
                );
            }

            logger.debug("Forwarding buffered opId {} to leader {}", req.getOpId(), currentLeader);
        }
    }

    // Check if the host is valid and belongs in static set of replicas
    private boolean isKnownMember(Host h) {
        return h != null && membership != null && membership.contains(h);
    }
}
