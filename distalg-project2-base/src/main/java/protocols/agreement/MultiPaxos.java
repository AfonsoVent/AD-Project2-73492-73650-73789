package protocols.agreement;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.messages.AcceptMessage;
import protocols.agreement.messages.AcceptNackMessage;
import protocols.agreement.messages.AcceptOKMessage;
import protocols.agreement.messages.BroadcastMessage;
import protocols.agreement.messages.PrepareMessage;
import protocols.agreement.messages.PrepareOKMessage;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.timer.AcceptRetryTimer;
import protocols.agreement.timer.PrepareRetryTimer;
import protocols.agreement.utils.Ballot;
import protocols.agreement.utils.PaxosSlot;
import protocols.agreement.utils.QuorumUtils;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * Multi-Paxos implementation with basic flow control, retry mechanisms and persistence.
 */
public class MultiPaxos extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(MultiPaxos.class);

    private static final int MAX_RETAINED_DECIDED_SLOTS = 4096;
    private static final int MAX_OUTSTANDING_WINDOW = 64;

    // Ballot
    private UUID ballotOwner;
    private Ballot promisedBallot;

    private Ballot currentBallot; // Current number ballot
    private Host currentLeader;// Who is the leader
    private boolean amILeader; // I'm the leader?

    // Phase 1
    private Set<Host> phase1Promises; // Who promised in this ballot
    private int majority;             // Size of the majority
    private Queue<ProposeRequest> pendingProposals;

    // Phase 2
    private int nextInstance; // Next instance to propose
    private Map<Integer, Set<Host>> acceptAcks;
    private Map<Integer, PrepareOKMessage.SlotStateData> gatheredAcceptedSlots;

    // Flow control / retries
    private final int maxOutstanding;
    private final long proposalIntervalMs;
    private final long acceptRetryInitialMs;
    private final double acceptRetryBackoff;

    private volatile int outstandingProposals = 0;
    private final Queue<ProposeRequest> proposalThrottleQueue = new ConcurrentLinkedQueue<>();

    // per-instance tracking for targeted retries
    private final Map<Integer, Set<Host>> acceptPendingTargets = new ConcurrentHashMap<>();
    private final Map<Integer, Long> acceptRetryDelay = new ConcurrentHashMap<>();

    // Persist reboot
    private Path stateFile;
    // async persistence control
    private final boolean disablePersistence;
    private final AtomicBoolean persistPending = new AtomicBoolean(false);
    private final Thread persistThread;

    // Timeouts
    private Map<Integer, Integer> acceptRetryGeneration;

    // Register of the slots
    private Map<Integer, PaxosSlot> slots;

    // Protocol information
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "MultiPaxos";

    // Information channel
    private Host myself;
    private int joinedInstance;
    private List<Host> membership;

    // primeiro lider
    private Map<Integer, Integer> prepareRetryGeneration; 
    private long prepareRetryDelayMs;

    public MultiPaxos(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        ballotOwner = UUID.randomUUID();
        stateFile = Paths.get("paxos_state_" + ballotOwner + ".dat");
        loadState();

        joinedInstance = -1; //-1 means we have not yet joined the system
        pendingProposals = new LinkedList<>();
        membership = new LinkedList<>();
        nextInstance = 0;
        acceptAcks = new HashMap<>();
        slots = new HashMap<>();
        gatheredAcceptedSlots = new HashMap<>();
        acceptRetryGeneration = new HashMap<>();
        prepareRetryGeneration = new HashMap<>();
        prepareRetryDelayMs = 1000L;

        promisedBallot = null;

        /* Flow-control and retry parameters from properties */
        int configuredMaxOutstanding = Integer.parseInt(props.getProperty("paxos.max_outstanding", "8"));
        this.maxOutstanding = Math.max(1, Math.min(configuredMaxOutstanding, MAX_OUTSTANDING_WINDOW));
        this.proposalIntervalMs = Long.parseLong(props.getProperty("paxos.proposal_interval_ms", "5"));
        this.acceptRetryInitialMs = Long.parseLong(props.getProperty("paxos.accept_retry_ms", "200"));
        this.acceptRetryBackoff = Double.parseDouble(props.getProperty("paxos.accept_retry_backoff", "2.0"));

        /* Persistence config: allow disabling for benchmarks or async writes */
        this.disablePersistence = Boolean.parseBoolean(props.getProperty("paxos.disable_persistence", "false"));
        if (this.disablePersistence) {
            this.persistThread = null;
        } else {
            this.persistThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        if (persistPending.getAndSet(false)) {
                            // coalesce writes and perform a synchronous write of current state
                            synchronized (MultiPaxos.this) {
                                try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(stateFile))) {
                                    oos.writeObject(promisedBallot);
                                    oos.writeObject(slots);
                                } catch (IOException e) {
                                    logger.warn("Failed to persist paxos state (async): {}", e.getMessage());
                                }
                            }
                        }
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }, "MultiPaxos-PersistThread");
            this.persistThread.setDaemon(true);
            this.persistThread.start();
            // ensure final flush on JVM shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                persistPending.set(true);
                if (persistThread != null) {
                    persistThread.interrupt();
                    try { persistThread.join(200); } catch (InterruptedException ignored) {}
                }
                synchronized (MultiPaxos.this) {
                    try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(stateFile))) {
                        oos.writeObject(promisedBallot);
                        oos.writeObject(slots);
                    } catch (IOException e) {
                        logger.warn("Failed to persist paxos state on shutdown: {}", e.getMessage());
                    }
                }
            }));
        }

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(AcceptRetryTimer.TIMER_ID, this::uponAcceptRetryTimer);
        registerTimerHandler(PrepareRetryTimer.TIMER_ID, this::uponPrepareRetryTimer);
        // Proposal throttle timer
        registerTimerHandler(ProposalThrottleTimer.TIMER_ID, this::uponProposalThrottleTimer);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {}

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        
        // Debug
        logger.info("Channel {} created, I am {}", cId, myself);
        
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        
        /*---------------------- Register Message Serializers ---------------------- */
        // Fase 1
        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(cId, PrepareOKMessage.MSG_ID, PrepareOKMessage.serializer);

        // Fase 2
        registerMessageSerializer(cId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageSerializer(cId, AcceptOKMessage.MSG_ID, AcceptOKMessage.serializer);
        registerMessageSerializer(cId, AcceptNackMessage.MSG_ID, AcceptNackMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
            // Fase 1
            registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareOKMessage.MSG_ID, this::uponPrepareOKMessage, this::uponMsgFail);
            
            // Fase 2
            registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptOKMessage.MSG_ID, this::uponAcceptOKMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptNackMessage.MSG_ID, this::uponAcceptNackMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }
    }

    // Fase 1

    private void uponPrepareMessage(PrepareMessage msg, Host from, short sourceProto, int channelId) {
        Ballot incoming = msg.getBallot();
        logger.debug("Received Prepare {} from {}", incoming, from);

        if (promisedBallot != null && incoming.compareTo(promisedBallot) <= 0) {
            logger.debug("Ignoring Prepare {} from {} because promisedBallot={}", incoming, from, promisedBallot);
            return;
        }

        promisedBallot = incoming;
        persistState();

        Map<Integer, PrepareOKMessage.SlotStateData> acceptedSlots = new HashMap<>();
        for (Map.Entry<Integer, PaxosSlot> entry : slots.entrySet()) {
            PaxosSlot slot = entry.getValue();

            if (slot.getHighestAcceptSeen() != null && slot.getAcceptedValue() != null) {
                acceptedSlots.put(entry.getKey(),
                        new PrepareOKMessage.SlotStateData(slot.getHighestAcceptSeen(), slot.getAcceptedValue()));
            }
        }

        sendMessage(new PrepareOKMessage(incoming, acceptedSlots), from);

        logger.debug("Sent PrepareOK {} -> {} (acceptedSlots={})", incoming, from, acceptedSlots.size());
    }

    private void uponPrepareOKMessage(PrepareOKMessage msg, Host from, short sourceProto, int channelId) {
        Ballot replyBallot = msg.getBallot();
        logger.debug("Received PrepareOK {} from {}", replyBallot, from);

        // Ignore older Ballots or Ballots from previous leadership attempts
        if (currentBallot == null || !replyBallot.equals(currentBallot)) {
            logger.debug("Ignoring PrepareOK {} from {} because currentBallot={}", replyBallot, from, currentBallot);
            return;
        }

        // Is the leader
        if (amILeader) return;

        // If necessary starts phase1
        if (phase1Promises == null) phase1Promises = new HashSet<>();
        if (gatheredAcceptedSlots == null) gatheredAcceptedSlots = new HashMap<>();

        for (Map.Entry<Integer, PrepareOKMessage.SlotStateData> e : msg.getAcceptedSlots().entrySet()) {
            int inst = e.getKey();
            PrepareOKMessage.SlotStateData incoming = e.getValue();
            gatheredAcceptedSlots.merge(inst, incoming, (oldV, newV) -> {
                if (oldV.highestAcceptSeen == null) return newV;
                if (newV.highestAcceptSeen == null) return oldV;
                return (newV.highestAcceptSeen.compareTo(oldV.highestAcceptSeen) > 0) ? newV : oldV;
            });
        }

        // Register phase1 promise, ignore duplicates
        if (!phase1Promises.add(from)) return;

        logger.debug("Phase1 ballot {} promises: {}/{}", currentBallot, phase1Promises.size(), majority);

        if (!QuorumUtils.hasMajority(phase1Promises.size(), membership.size())) return;

        // We have quorum — integrate accumulated accepted slots into local slots
        for (Map.Entry<Integer, PrepareOKMessage.SlotStateData> entry : gatheredAcceptedSlots.entrySet()) {
            int instance = entry.getKey();
            PrepareOKMessage.SlotStateData slotData = entry.getValue();

            PaxosSlot slot = slots.computeIfAbsent(instance, k -> new PaxosSlot());

            if (slotData.highestAcceptSeen != null) {
                Ballot localAccept = slot.getHighestAcceptSeen();
                if (localAccept == null || slotData.highestAcceptSeen.compareTo(localAccept) > 0) {
                    slot.setHighestAcceptSeen(slotData.highestAcceptSeen);
                    slot.setAcceptedValue(slotData.value);
                }
            }
        }

        // recomputar nextInstance com base nos slots integrados
        nextInstance = computeNextInstanceFromSlots();
        persistState();

        // limpar acumulador para não reutilizar em rounds futuros
        gatheredAcceptedSlots.clear();

        // finalize liderança
        amILeader = true;
        currentLeader = myself;
        promisedBallot = currentBallot;
        triggerNotification(new LeaderChangeNotification(myself));

        prepareRetryGeneration.put(0, prepareRetryGeneration.getOrDefault(0,0) + 1);
        logger.info("Became leader with ballot {}", currentBallot);

        prepareRetryDelayMs = 1000L;
        processPendingProposalsAsLeader();
    }

    private void startPhase1() {
        outstandingProposals = 0;
        acceptAcks.clear();
        acceptPendingTargets.clear();
        acceptRetryDelay.clear();
        acceptRetryGeneration.clear();
        updateKnownLeader(null);

        if (membership == null || membership.isEmpty()) {
            logger.debug("Cannot start Phase 1 without membership");
            return;
        }

        if (phase1Promises == null) {
            phase1Promises = new HashSet<>();
        } else {
            phase1Promises.clear();
        }

        if (gatheredAcceptedSlots != null) gatheredAcceptedSlots.clear();

        amILeader = false;
        currentLeader = null;

        if (currentBallot == null) {
            currentBallot = Ballot.initial(ballotOwner);
        } else {
            currentBallot = currentBallot.next();
        }

        logger.info("Starting Phase 1 with ballot {}", currentBallot);

        // incrementar geração e agendar retry com backoff para evitar eleições sincronizadas
        int gen = prepareRetryGeneration.getOrDefault(0, 0) + 1;
        prepareRetryGeneration.put(0, gen);

        // enviar prepare uma única vez
        PrepareMessage prepare = new PrepareMessage(currentBallot);
        membership.forEach(host -> sendMessage(prepare, host));

        long retryDelayMs = Math.min(prepareRetryDelayMs + ThreadLocalRandom.current().nextLong(250), 5000L);
        prepareRetryDelayMs = Math.min(Math.max(prepareRetryDelayMs * 2, 1000L), 5000L);
        setupTimer(new PrepareRetryTimer(gen), retryDelayMs);
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());

        recalcMajority();

        if (slots == null) {
            slots = new HashMap<>();
        }

        if (phase1Promises == null) {
            phase1Promises = new HashSet<>();
        }

        majority = QuorumUtils.majority(membership.size());
        nextInstance = computeNextInstanceFromSlots();
        prepareRetryDelayMs = 1000L + (long) Math.max(0, membership.indexOf(myself)) * 1000L;

        logger.info("Agreement starting at instance {}, membership: {}, majority: {}",
                joinedInstance, membership, majority);

        if (membership.indexOf(myself) <= 0) {
            startPhase1();
        } else {
            int gen = prepareRetryGeneration.getOrDefault(0, 0) + 1;
            prepareRetryGeneration.put(0, gen);
            setupTimer(new PrepareRetryTimer(gen), prepareRetryDelayMs);
        }
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.info("PROPOSE in opId={} instance={} leader={} ballot={}",
                request.getOpId(), request.getInstance(), amILeader, currentBallot);

        if (joinedInstance < 0) {
            logger.debug("Still joining, buffering proposal {}", request.getOpId());
            pendingProposals.add(request);
            return;
        }

        if (!amILeader) {
            logger.debug("Not leader yet, buffering proposal {} and starting Phase 1 if needed", request.getOpId());
            pendingProposals.add(request);

            if (currentBallot == null) startPhase1();
            return;
        }

        int instance = nextInstance++;
        logger.info("PROPOSE -> ACCEPT opId={} instance={} ballot={}", request.getOpId(), instance, currentBallot);
        leaderSendAccept(instance, request.getOpId(), request.getOperation());

        logger.debug("I am leader, proposal {} can proceed to Accept phase", request.getOpId());
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        membership.add(request.getReplica());
        recalcMajority();
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        membership.remove(request.getReplica());
        recalcMajority();
    }

    // Fase 2
    private void uponAcceptMessage(AcceptMessage msg, Host from, short sourceProto, int channelId) {
        Ballot ballot = msg.getBallot();
        int instance = msg.getInstance();
        UUID opId = msg.getOpId();
        byte[] value = msg.getValue();

        logger.debug("ACCEPT in ballot={} instance={} from={} opId={}", ballot, instance, from, opId);

        if (promisedBallot != null && ballot.compareTo(promisedBallot) < 0) {
            logger.debug("Rejecting Accept(ballot={}, instance={}) from {} because promisedBallot={}",
                    ballot, instance, from, promisedBallot);

            sendMessage(new AcceptNackMessage(promisedBallot, instance), from);
            return;
        }

        updateKnownLeader(from);
        promisedBallot = ballot;
        persistState();

        if (slots == null) slots = new HashMap<>();

        PaxosSlot slot = slots.computeIfAbsent(instance, k -> new PaxosSlot());
        slot.setHighestAcceptSeen(ballot);
        slot.setOpId(opId);
        slot.setAcceptedValue(value);
        persistState();
        
        nextInstance = computeNextInstanceFromSlots();

        sendMessage(new AcceptOKMessage(ballot, instance), from);

        logger.debug("ACCEPT-OK sent ballot={} instance={} to={}", ballot, instance, from);
    }
    
    private void uponAcceptOKMessage(AcceptOKMessage msg, Host from, short sourceProto, int channelId) {
        Ballot ballot = msg.getBallot();
        int instance = msg.getInstance();

        if (currentBallot == null || !ballot.equals(currentBallot)) return;
        if (!amILeader) return;

        Set<Host> acks = acceptAcks.computeIfAbsent(instance, k -> new HashSet<>());
        if (!acks.add(from)) return;

        // remove from pending targets for targeted retries
        Set<Host> remaining = acceptPendingTargets.get(instance);
        if (remaining != null) remaining.remove(from);

        logger.debug("ACCEPT-ACK ballot={} instance={} from={} acks={}/{}",
            ballot, instance, from, acks.size(), membership.size());

        if (!QuorumUtils.hasMajority(acks.size(), membership.size())) return;

        // Mark decided
        PaxosSlot slot = slots.get(instance);
        if (slot == null || slot.getIsDecided()) {
            acceptAcks.remove(instance);
            return;
        }

        slot.setDecided(true);
        acceptRetryGeneration.remove(instance);
        // clear retry tracking for this instance
        acceptPendingTargets.remove(instance);
        acceptRetryDelay.remove(instance);
        acceptAcks.remove(instance);
        persistState();
        logger.info("DECIDED instance={} opId={} ballot={}", instance, slot.getOpId(), ballot);
        triggerNotification(new DecidedNotification(instance, slot.getOpId(), slot.getAcceptedValue()));
        // decrease outstanding proposals and try to drain pending queue
        outstandingProposals = Math.max(0, outstandingProposals - 1);
        processPendingProposalsAsLeader();
    }

    private void uponAcceptNackMessage(AcceptNackMessage msg, Host from, short sourceProto, int channelId) {
        Ballot promised = msg.getPromised();
        logger.debug("Received AcceptNack promised={} from {}", promised, from);

        // se promised maior que o nosso currentBallot, atualizar e reiniciar Phase1
        if (currentBallot == null || promised.compareTo(currentBallot) > 0) {
            // escolher próximo ballot maior que promised (por exemplo promised.next())
            currentBallot = promised.next();
            // limpar e reiniciar Phase1 para recuperar liderança
            if (phase1Promises != null) phase1Promises.clear();
            if (gatheredAcceptedSlots != null) gatheredAcceptedSlots.clear();

            acceptRetryGeneration.clear();

            startPhase1();
        }
    }

    private void leaderSendAccept(int instance, UUID opId, byte[] value) {
        AcceptMessage msg = new AcceptMessage(currentBallot, instance, opId, value);
        // garantir slot existente
        PaxosSlot slot = slots.computeIfAbsent(instance, k -> new PaxosSlot());

        slot.setHighestAcceptSeen(currentBallot);
        slot.setOpId(opId);
        slot.setAcceptedValue(value);

        // enviar a todos
        logger.debug("SEND ACCEPT instance={} ballot={} targets={}", instance, currentBallot, membership.size());
        for (Host h : membership) {
            sendMessage(msg, h);
        }
        // também conta o próprio ack
        acceptAcks.computeIfAbsent(instance, k -> new HashSet<>()).add(myself);

        // track pending targets for targeted retries
        acceptPendingTargets.put(instance, new HashSet<>(membership));
        acceptRetryDelay.put(instance, acceptRetryInitialMs);

        // increment outstanding proposals (flow control)
        outstandingProposals++;

        // leader counts itself as acked, so don't retry to self
        Set<Host> pending = acceptPendingTargets.get(instance);
        if (pending != null) pending.remove(myself);

        scheduleAcceptRetry(instance, opId, value, (int) acceptRetryInitialMs);
        persistState();
    }

    private void processPendingProposalsAsLeader() {
        // drain pending proposals but limit outstanding proposals to window
        while (outstandingProposals < maxOutstanding) {
            ProposeRequest req = pendingProposals.poll();
            if (req == null) break;
            int instance = nextInstance++;
            leaderSendAccept(instance, req.getOpId(), req.getOperation());
        }

        // if still queued, schedule a throttle timer to continue draining
        if (!pendingProposals.isEmpty()) {
            setupTimer(new ProposalThrottleTimer(), proposalIntervalMs);
        }
    }

    // Tolerate reboot
    private synchronized void persistState() {
        // If persistence disabled (benchmark mode), skip writes
        if (disablePersistence) return;

        // Coalesce frequent calls: mark pending and let background thread perform the write
        persistPending.set(true);
    }

    private synchronized void loadState() {
        if (!Files.exists(stateFile)) return;
        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(stateFile))) {
            promisedBallot = (Ballot) ois.readObject();
            Object s = ois.readObject();
            if (s instanceof Map) slots = (Map<Integer, PaxosSlot>) s;
        } catch (Exception e) {
            logger.warn("Failed to load paxos state: {}", e.getMessage());
        }
    }

    // Timeouts
    private void scheduleAcceptRetry(int instance, UUID opId, byte[] value, long delayMs) {
        int generation = acceptRetryGeneration.getOrDefault(instance, 0) + 1;
        acceptRetryGeneration.put(instance, generation);

        setupTimer(new AcceptRetryTimer(instance, currentBallot, opId, value, generation, delayMs), delayMs);
    }

    private void uponAcceptRetryTimer(AcceptRetryTimer timer, long timerId) {
        if (currentBallot == null || !currentBallot.equals(timer.getBallot())) return;
        if (!amILeader) return;

        Integer currentGeneration = acceptRetryGeneration.get(timer.getInstance());
        if (currentGeneration == null || currentGeneration != timer.getGeneration()) return;

        PaxosSlot slot = slots.get(timer.getInstance());
        if (slot != null && slot.getIsDecided()) return;

        // Reenvia e agenda o próximo retry pelo mesmo fluxo do Babel
        logger.debug("ACCEPT retry instance={} ballot={} gen={}", timer.getInstance(), timer.getBallot(), timer.getGeneration());

        // send only to hosts that have not acked yet
        Set<Host> remaining = acceptPendingTargets.get(timer.getInstance());
        if (remaining == null || remaining.isEmpty()) return;

        AcceptMessage retryMsg = new AcceptMessage(timer.getBallot(), timer.getInstance(), timer.getOpId(), timer.getValue());
        for (Host h : new HashSet<>(remaining)) {
            sendMessage(retryMsg, h);
        }

        // exponential backoff for next retry
        long nextDelay = (long) (timer.getDelayMs() * acceptRetryBackoff);
        acceptRetryDelay.put(timer.getInstance(), nextDelay);
        scheduleAcceptRetry(timer.getInstance(), timer.getOpId(), timer.getValue(), (int) nextDelay);
    }

    private void updateKnownLeader(Host leader) {
        if (leader == null) {
            if (currentLeader != null) {
                currentLeader = null;
                triggerNotification(new LeaderChangeNotification(null));
            }
            return;
        }

        if (leader.equals(currentLeader)) {
            return;
        }

        currentLeader = leader;
        triggerNotification(new LeaderChangeNotification(leader));
    }

    // Primeiro lider
    private void uponPrepareRetryTimer(PrepareRetryTimer timer, long timerId) {
        int gen = prepareRetryGeneration.getOrDefault(0, 0);
        if (timer.getGeneration() != gen) return; // timer obsoleto
        if (amILeader) return; // já temos líder

        // If we already know a leader, let it continue instead of constantly restarting elections.
        // This avoids the livelock where followers keep preempting a stable leader.
        if (currentLeader != null) {
            logger.debug("Skipping Phase 1 retry because leader {} is known", currentLeader);
            return;
        }

        // não somos líder ainda — incrementa ballot e reinicia Phase1
        if (currentBallot == null) currentBallot = Ballot.initial(ballotOwner);
        else currentBallot = currentBallot.next();

        logger.info("Prepare retry generation {}, starting new Phase1 with {}", gen, currentBallot);
        prepareRetryDelayMs = Math.min(Math.max(prepareRetryDelayMs * 2, 1000L), 5000L);
        startPhase1();
    }

    // Proposal throttle timer (single-shot) to space out draining pending proposals
    private class ProposalThrottleTimer extends pt.unl.fct.di.novasys.babel.generic.ProtoTimer {
        public static final short TIMER_ID = 404;
        public ProposalThrottleTimer() { super(TIMER_ID); }
        @Override public pt.unl.fct.di.novasys.babel.generic.ProtoTimer clone() { return this; }
    }

    private void uponProposalThrottleTimer(ProposalThrottleTimer timer, long timerId) {
        // drain pending proposals up to window
        processPendingProposalsAsLeader();
    }

    // Auxiliar function
    private void recalcMajority() {
        if (membership == null) {
            majority = 0;
        } else {
            majority = QuorumUtils.majority(membership.size());
        }
    }

    private int computeNextInstanceFromSlots() {
        if (slots == null || slots.isEmpty()) return joinedInstance >= 0 ? joinedInstance : 0;
        int max = -1;
        for (Integer k : slots.keySet()) if (k > max) max = k;
        int base = Math.max(max + 1, joinedInstance >= 0 ? joinedInstance : 0);
        return base;
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }
}
