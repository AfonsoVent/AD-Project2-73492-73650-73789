package protocols.agreement;

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

import protocols.agreement.messages.AcceptMessage;
import protocols.agreement.messages.AcceptOKMessage;
import protocols.agreement.messages.PrepareMessage;
import protocols.agreement.messages.PrepareOKMessage;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.StealLeaderRequest;
import protocols.agreement.utils.PaxosSlot;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.notifications.ClientRequestReply;
import protocols.statemachine.requests.OrderRequest;
import protocols.statemachine.StateMachine;
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

public class MultiPaxos extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(StateMachine.class);
    public static final String PROTOCOL_NAME = "MultiPaxos";
    public static final short PROTOCOL_ID = 100;

    private final Host self;
    private int channelId;
    private List<Host> membership;

    // Estado multiPaxos
    private int currentTerm;
    private boolean isLeader;
    private final Map<Integer, PaxosSlot> log;

    // Vars fase 1
    private final Set<Host> prepareReceiveds;
    private final Map<Integer, PrepareOKMessage.SlotStateData> highestValuesFromPrepares;

    // Vars fase 2
    private final Map<Integer, Set<Host>> acceptQuorums;

    public MultiPaxos(Properties props) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.log = new HashMap<>();
        this.currentTerm = 0;
        this.isLeader = false;

        this.prepareReceiveds = new HashSet<>();
        this.highestValuesFromPrepares = new HashMap<>();
        this.acceptQuorums = new HashMap<>();

        // Usar o endereço IP/Porta configurado no Babel
        String address = props.getProperty("babel.address");
        int port = Integer.parseInt(props.getProperty("babel.port"));
        this.self = new Host(address, port);

        // Subscrever a notificação de rede enviada pela StateMachine
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelReady);
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelReady);
    }

    @Override
    public void init(Properties props) {}

    private void uponChannelReady(ChannelReadyNotification notification, short sourceProto) {
        this.channelId = notification.getChannelId();
        
        registerMessageSerializer(channelId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(channelId, PrepareOKMessage.MSG_ID, PrepareOKMessage.serializer);

        try {
            registerMessageHandler(channelId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(channelId, PrepareOKMessage.MSG_ID, this::uponPrepareOKMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Erro ao registar handlers do MultiPaxos", e);
        }
    }

    private void startPhase1() {
        // Incrementar o termo para ser maior que qualquer um visto anteriormente
        this.currentTerm += 1; 
        this.isLeader = false;
        this.prepareReceiveds.clear();
        this.highestValuesFromPrepares.clear();

        logger.info("A iniciar Fase 1 (Prepare) para o Termo: {}", currentTerm);

        PrepareMessage msg = new PrepareMessage(currentTerm);
        
        // Enviar a todos os pares da rede
        for (Host peer : membership) {
            if (!peer.equals(self)) {
                sendMessage(msg, peer);
            }
        }
        
        // Adicionar a nossa própria promessa automaticamente
        this.prepareReceiveds.add(self);
        checkPrepareQuorum();
    }

    private void uponPrepareOKMessage(PrepareOKMessage msg, Host from, short sourceProto, int channelId) {
        // Ignorar respostas antigas ou atrasadas
        if (msg.getTerm() != this.currentTerm || this.isLeader) return;

        this.prepareReceiveds.add(from);
        logger.debug("Recebido PrepareOK de {}. Total promessas: {}/{}", from, prepareReceiveds.size(), membership.size());

        // Processar os valores que a réplica já aceitou no passado para recuperar o estado
        for (Map.Entry<Integer, PrepareOKMessage.SlotStateData> entry : msg.getAcceptedSlots().entrySet()) {
            int slotId = entry.getKey();
            PrepareOKMessage.SlotStateData incomingData = entry.getValue();

            PrepareOKMessage.SlotStateData existingData = highestValuesFromPrepares.get(slotId);
            if (existingData == null || incomingData.highestAcceptSeen > existingData.highestAcceptSeen) {
                highestValuesFromPrepares.put(slotId, incomingData);
            }
        }

        checkPrepareQuorum();
    }

    private void checkPrepareQuorum() {
        int quorumSize = (membership.size() / 2) + 1;
        if (prepareReceiveds.size() >= quorumSize && !this.isLeader) {
            this.isLeader = true;
            logger.info("=== GANHOU A LIDERANÇA no termo {}! ===", currentTerm);

            // 1. Atualizar o log local com os maiores valores recuperados das promessas do quórum
            for (Map.Entry<Integer, PrepareOKMessage.SlotStateData> entry : highestValuesFromPrepares.entrySet()) {
                int slotId = entry.getKey();
                PrepareOKMessage.SlotStateData data = entry.getValue();
                
                if (data.value != null) {
                    PaxosSlot slot = log.computeIfAbsent(slotId, k -> new PaxosSlot());
                    slot.setHighestAcceptSeen(data.highestAcceptSeen);
                    slot.setAcceptedValue(data.value);
                    // Importante: O líder terá de correr a Fase 2 (Accept) para re-confirmar estes valores recuperados!
                }
            }

            // 2. Avisar a StateMachine local que passámos a ser o líder
            triggerNotification(new LeaderChangeNotification(self));
        }
    }

    /* -------------------------------------------------------------------------------------
     * LOGICA DA FASE 1 - ACCEPTOR (SEGUIDOR)
     * ------------------------------------------------------------------------------------- */
    
    private void uponPrepareMessage(PrepareMessage msg, Host from, short sourceProto, int channelId) {
        // Regra de Ouro do Paxos: Só aceitamos termos estritamente maiores do que o que já vimos
        if (msg.getTerm() > this.currentTerm) {
            this.currentTerm = msg.getTerm();
            this.isLeader = false; // Se achávamos que éramos líderes, já não somos
            
            logger.info("Promessa dada ao nó {} para o termo {}", from, currentTerm);
            
            // Notificar a StateMachine local sobre a potencial mudança de líder
            triggerNotification(new LeaderChangeNotification(from));

            // Empacotar todos os slots que já têm algum valor aceito para enviar ao líder
            Map<Integer, PrepareOKMessage.SlotStateData> acceptedSlots = new HashMap<>();
            for (Map.Entry<Integer, PaxosSlot> entry : log.entrySet()) {
                PaxosSlot slot = entry.getValue();
                if (slot.getAcceptedValue() != null) {
                    acceptedSlots.put(entry.getKey(), 
                        new PrepareOKMessage.SlotStateData(slot.getHighestAcceptSeen(), slot.getAcceptedValue()));
                }
                // Aproveitamos para atualizar o termo visto no slot local
                slot.setHighestPrepareSeen(currentTerm);
            }

            // Responder ao Proposer
            sendMessage(new PrepareOKMessage(currentTerm, acceptedSlots), from);
        } else {
            logger.debug("Prepare rejeitado de {} (termo {} <= atual {})", from, msg.getTerm(), currentTerm);
        }
    }

    /* -------------------------------------------------------------------------------------
     * LOGICA DA FASE 2 (Accept) - PROPOSER (LÍDER)
     * ------------------------------------------------------------------------------------- */
    
    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        // Se não formos o líder, ignoramos o pedido (a StateMachine trata de reencaminhar)
        if (!isLeader) {
            logger.warn("Recebido ProposeRequest mas NÃO sou o líder.");
            return;
        }

        int instance = request.getInstance();
        byte[] operation = request.getOperation();

        logger.info("Líder a iniciar Fase 2 para a instância/slot {} no termo {}", instance, currentTerm);

        // 1. Atualizar o log local do próprio líder
        PaxosSlot slot = log.computeIfAbsent(instance, k -> new PaxosSlot());
        slot.setHighestAcceptSeen(currentTerm);
        slot.setAcceptedValue(operation);

        // 2. Inicializar o quórum de aceitação para este slot e adicionar o voto do líder
        Set<Host> acceptedHosts = new HashSet<>();
        acceptedHosts.add(self);
        acceptQuorums.put(instance, acceptedHosts);

        // 3. Disparar a AcceptMessage para todas as outras réplicas
        AcceptMessage msg = new AcceptMessage(currentTerm, instance, operation);
        for (Host peer : membership) {
            if (!peer.equals(self)) {
                sendMessage(msg, peer);
            }
        }

        // No caso de termos apenas 1 nó na rede (quórum = 1)
        checkAcceptQuorum(instance);
    }

    private void uponAcceptOKMessage(AcceptOKMessage msg, Host from, short sourceProto, int channelId) {
        // Ignorar se a mensagem for de um termo antigo ou se já não formos líderes
        if (msg.getTerm() != this.currentTerm || !this.isLeader) return;

        int instance = msg.getInstance();
        Set<Host> acceptedHosts = acceptQuorums.get(instance);
        
        if (acceptedHosts != null) {
            acceptedHosts.add(from);
            logger.debug("Recebido AcceptOK de {} para a instância {}. Total: {}/{}", 
                    from, instance, acceptedHosts.size(), membership.size());
            
            checkAcceptQuorum(instance);
        }
    }

    private void checkAcceptQuorum(int instance) {
        Set<Host> acceptedHosts = acceptQuorums.get(instance);
        if (acceptedHosts == null) return;

        int quorumSize = (membership.size() / 2) + 1;
        PaxosSlot slot = log.get(instance);

        // Se alcançámos a maioria e a instância ainda não está decidida
        if (acceptedHosts.size() >= quorumSize && slot != null && !slot.isDecided()) {
            slot.setDecided(true);
            logger.info("=== INSTÂNCIA {} DECIDIDA! ===", instance);

            // Remover do mapa de quóruns ativos para poupar memória
            acceptQuorums.remove(instance);

            // IMPORTANTE: Responder à StateMachine com a DecidedNotification
            // Passamos o ID da instância e a operação consolidada
            triggerNotification(new DecidedNotification(instance, slot.getAcceptedValue(), ));
        }
    }

    /* -------------------------------------------------------------------------------------
     * LOGICA DA FASE 2 (Accept) - ACCEPTOR (SEGUIDOR)
     * ------------------------------------------------------------------------------------- */
    
    private void uponAcceptMessage(AcceptMessage msg, Host from, short sourceProto, int channelId) {
        int instance = msg.getInstance();
        PaxosSlot slot = log.computeIfAbsent(instance, k -> new PaxosSlot());

        // Regra do Acceptor: Aceita se o termo for MAIOR OU IGUAL ao maior prepare que já viu
        if (msg.getTerm() >= slot.getHighestPrepareSeen() && msg.getTerm() >= this.currentTerm) {
            
            // Atualizar o termo atual se ficámos para trás
            if (msg.getTerm() > this.currentTerm) {
                this.currentTerm = msg.getTerm();
                this.isLeader = false;
                triggerNotification(new LeaderChangeNotification(from));
            }

            // Gravar o valor aceito no log
            slot.setHighestAcceptSeen(msg.getTerm());
            slot.setHighestPrepareSeen(msg.getTerm());
            slot.setAcceptedValue(msg.getValue());

            logger.info("Instância {} aceite do líder {} no termo {}", instance, from, msg.getTerm());

            // Responder de volta ao líder confirmando a gravação
            sendMessage(new AcceptOKMessage(this.currentTerm, instance), from);
        } else {
            logger.warn("Accept de {} REJEITADO para a instância {}. Termo msg: {} | Termo slot visto: {}", 
                    from, instance, msg.getTerm(), slot.getHighestPrepareSeen());
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Mensagem {} para {} falhou: {}", msg.getClass().getSimpleName(), host, throwable.getMessage());
    }
}
