package protocols.statemachine;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.IncorrectAgreement;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.notifications.ClientRequestReply;
import protocols.statemachine.requests.OrderRequest;
import protocols.statemachine.messages.ForwardOpMessage;
import protocols.statemachine.timers.ReconnectTimer;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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

    // To [Leader Fowarding]
    private Host currentLeader; // null if unknow

    // To [Connection Management]
    private final Set<Host> connectedPeers = new HashSet<>(); // Current active TCP connections
    private final Set<Host> reconnectScheduled = new HashSet<>(); // Used to scheduling duplicate reconnect timers
    private final Map<Host, Queue<ForwardOpMessage>> outboundBuffer = new HashMap<>(); // Used when leader is unreachable
    private static final long RECONNECT_BASE_MS = 200; // Initial time to reconnect
    private static final long RECONNECT_MAX_MS = 5000; // Max time to reconnect
    private final Map<Host, Long> reconnectDelay = new HashMap<>(); // Used to get the time is getting to reconnect

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        
        // ordering
        nextExecuteInstance = 0;
        decidedBuffer = new HashMap<>();

        // Starting the currentLeader is null until someone tells that isn't
        this.currentLeader = null;

        // Proposal instance
        this.nextInstance = 0;

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
        logger.debug("Received request: " + request);
        if (state == State.JOINING) {
            //Do something smart (like buffering the requests)
            return;
        } 
        
        if (currentLeader == null) {
            //Do something even more smart (like buffering until reconhecer leader :P)
            return;
        }

        if (self.equals(currentLeader)) {
            sendRequest(new ProposeRequest(nextInstance++, request.getOpId(), request.getOperation()),
                    IncorrectAgreement.PROTOCOL_ID);
        } else {
            // Send(FwMsg(Id, Op), IdLeader)
            sendOrBufferToHost(new ForwardOpMessage(request.getOpId(), request.getOperation()), currentLeader);
        }
    }

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.debug("Received notification: {}", notification);
        int instance = notification.getInstance();
        
        // Catch already executed instance (number less then actual instance expected)
        if (instance < nextExecuteInstance) {
            logger.debug("Ignoring because was already executed the instance {}", instance);
            return;
        }

        // No duplication
        decidedBuffer.putIfAbsent(instance, notification);

        tryExecuteInOrder();
    }
    
    private void uponLeaderChangeNotification(LeaderChangeNotification notification, short sourceProto) {
        // Get the ID from notification, and save it
        this.currentLeader = notification.getLeaderID();
        logger.info("Received notification:" + currentLeader);
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
        
        // Send(Propose(Instance, id, Op), )
        sendRequest(new ProposeRequest(nextInstance++, msg.getOpId(), msg.getOperation()), 
            IncorrectAgreement.PROTOCOL_ID);
    }

    private void sendOrBufferToHost(ForwardOpMessage msg, Host dst) {
        // Check destination(dst)
        if (dst == null) return;
        
        if (connectedPeers.contains(dst)) {
            // Send msg to destination
            sendMessage(msg, dst);
        } else {
            // Peer unavailable; buffer message
            outboundBuffer.computeIfAbsent(dst, h -> new LinkedList<>()).add(msg);
            
            // Trigger reconnect workflow
            ensureReconnect(dst);
        }
    }

    private void ensureReconnect(Host h) {
        // Ignore invalid targets
        if (h == null || h.equals(self)) return;
        
        // Reconnect with only know members
        if (!membership.contains(h)) return;

        // Avoid scheduling duplicate reconnect timers
        if (reconnectScheduled.contains(h)) return;
    
        reconnectScheduled.add(h);

        // Get delay time
        long delay = reconnectDelay.getOrDefault(h, RECONNECT_BASE_MS);
        
        // Schedule reconnect attempt in the future
        setupTimer(new ReconnectTimer(h), delay);
    }

    private void uponReconnectTimer(ReconnectTimer timer, long timerId) {
        Host h = timer.getTarget();
        
        // Timer failed
        reconnectScheduled.remove(h);
        
        // If it's invalid to reconnect, don't need it
        if (h == null || !membership.contains(h) || connectedPeers.contains(h)) return;
        
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
            triggerNotification(new ClientRequestReply(next.getOpId(), next.getOperation()));
            
            // Update nextExecuteInstance
            nextExecuteInstance++;
        }
    }
}
