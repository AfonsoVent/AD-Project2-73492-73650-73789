# Multi-Paxos Agreement Protocol Implementation

## Overview

This document describes the **MultiPaxosAgreement.java** implementation, a distributed consensus protocol that replicates a key-value store across multiple replicas in a Byzantine-tolerant manner. The implementation follows the **Multi-Paxos** algorithm as specified in your project requirements, using the **Babel framework** for inter-replica communication.

### Key Characteristics

- **Consensus Protocol**: Multi-Paxos with leader-based optimization
- **Network**: TCP-based communication via Babel's TCPChannel
- **Fault Tolerance**: Tolerates up to ⌊(n-1)/2⌋ crash faults with majority quorums
- **Framework**: Babel v2.0+ (distributed protocol framework)
- **Java Version**: Java 11+

---

## Algorithm Overview

Multi-Paxos is executed in **three phases** (leader-optimized):

### Phase 1: Prepare (Leadership Establishment)

Executed **once per leader term** to establish leadership and recover previously accepted values.

**Proposer Algorithm:**

```
while(true) do
  choose unique ballot sn, higher than any n seen so far
  send PREPARE(sn) to all acceptors
  if PREPARE_OK(na, va) from majority then
    break  // Phase 1 complete - proceed to Phase 2
  else
    continue  // timeout, retry with higher ballot
```

**Acceptor Algorithm:**

```
State: np (highest prepare seen)
PREPARE(n):
  if n > np then
    np = n
    reply <PREPARE_OK, na, va> where (na, va) = (highest ballot accepted, value)
```

---

### Phase 2: Accept (Value Proposal)

Executed **once per slot** to replicate a command across the cluster.

**Proposer Algorithm:**

```
for each slot with proposed value v do
  if not phase1Complete then
    goto Phase 1

  va = va with highest na (or v otherwise)  // Value recovery
  send ACCEPT(sn, va) to all acceptors

  if ACCEPT_OK(n) from majority then
    send DECIDED(va) to all learners
    break
  else
    continue  // timeout, retry Phase 1 with higher ballot
```

**Acceptor Algorithm:**

```
State: np, na, va
ACCEPT(n, v):
  if n >= np then
    na = n
    va = v
    np = n
    reply <ACCEPT_OK, n>
```

---

### Phase 3: Learn (Decision Notification)

Learners accumulate ACCEPT_OK votes and commit when majority is reached.

**Learner Algorithm:**

```
State: decision, na, va, aset (acceptor set)
upon ACCEPT_OK(n, v) from acceptor a:
  if n > na then
    na = n
    va = v
    aset.reset()
  else if n == na then
    aset.add(a)
    if aset is (majority) quorum then
      decision = va
      notify upper layer
```

---

## Implementation Details

### 1. **Ballot Management**

**Ballot Structure**: `Ballot(round, proposer)`

- `round`: Monotonically increasing ballot number
- `proposer`: Host ID of the proposer (tie-breaker)

**Comparison**: Lexicographic ordering by (round, proposer_id)

```java
Ballot ballot1 = new Ballot(10, host1);
Ballot ballot2 = new Ballot(10, host2);
// ballot1 < ballot2 if host1_id < host2_id
```

### 2. **Per-Instance State Management**

Each proposal slot maintains independent state via `SlotState`:

```java
class SlotState {
    Ballot acceptedBallot;      // Highest ballot this replica accepted for this instance
    UUID acceptedOpId;           // Operation ID of accepted value
    byte[] acceptedOp;           // The actual operation data
    Ballot phase2Ballot;         // Current Phase 2 ballot (proposer's ballot)
    Set<Host> acceptReplies;     // Acceptors that voted OK for phase2Ballot
    boolean isDecided;           // Whether value is decided
}
```

### 3. **Value Recovery Mechanism**

**Problem**: After leader crash, how does new leader know what values were previously accepted?

**Solution**: During Phase 1, new leader asks all acceptors "what's the highest value you've accepted?" and recovers it.

**Implementation**:

```java
// In handlePrepareReplyMessage():
for each AcceptedValue in PREPARE_OK reply:
    keep the one with highest ballot (na)

// Later in acceptProposal():
if (recoveredValues.contains(instance)) {
    use recovered value  // Safety: ensures consistency
} else {
    use proposed value   // Liveness: allow new commands
}
```

This ensures **safety**: previously accepted values are recovered and re-proposed, maintaining invariant that all instances on same slot have same value.

### 4. **Learner Ballot Tracking**

**Problem**: Multiple proposers might propose different values. How does learner know which votes to count?

**Solution**: Learner tracks which ballot is being considered and resets vote count if higher ballot seen.

**Implementation**:

```java
// In handleAcceptReplyMessage():
if (ballot > slot.phase2Ballot) {
    slot.phase2Ballot = ballot  // Update to higher ballot
    slot.acceptReplies.reset()   // Reset vote count (learner rule)
}

// Only count votes for current phase2Ballot
if (ballot == slot.phase2Ballot) {
    acceptReplies.add(sender)
}

if (acceptReplies.size() >= majority) {
    decide(value)
}
```

This prevents **split-brain**: even if two proposers propose for same slot, learner correctly waits for majority of votes for **same ballot**.

---

## Message Types

### Prepare Phase

| Message               | Direction            | Payload                           | Purpose                           |
| --------------------- | -------------------- | --------------------------------- | --------------------------------- |
| `PrepareMessage`      | Proposer → Acceptors | `Ballot`                          | Request permission to lead        |
| `PrepareReplyMessage` | Acceptor → Proposer  | `Ballot, ok, List<AcceptedValue>` | Grant leadership + recover values |

### Accept Phase

| Message              | Direction            | Payload                             | Purpose                     |
| -------------------- | -------------------- | ----------------------------------- | --------------------------- |
| `AcceptMessage`      | Proposer → Acceptors | `Ballot, instance, opId, operation` | Propose value for slot      |
| `AcceptReplyMessage` | Acceptor → Proposer  | `Ballot, instance, ok`              | Accept value (learner vote) |

### Decide Phase

| Message                     | Direction      | Payload                             | Purpose             |
| --------------------------- | -------------- | ----------------------------------- | ------------------- |
| `DecideMessage`             | Proposer → All | `Ballot, instance, opId, operation` | Notify decision     |
| `LeaderAnnouncementMessage` | Proposer → All | `Ballot`                            | Announce new leader |

---

## Request Types

| Request                | Source                     | Meaning                           |
| ---------------------- | -------------------------- | --------------------------------- |
| `ProposeRequest`       | Upper layer (StateMachine) | New command to replicate for slot |
| `StealLeaderRequest`   | Upper layer                | Force leadership change           |
| `AddReplicaRequest`    | External                   | Add new replica (not implemented) |
| `RemoveReplicaRequest` | External                   | Remove replica (not implemented)  |

---

## Notification Types

| Notification               | Recipient                  | Meaning                                |
| -------------------------- | -------------------------- | -------------------------------------- |
| `DecidedNotification`      | Upper layer (StateMachine) | Slot has been decided; apply operation |
| `LeaderChangeNotification` | Upper layer                | New leader elected                     |

---

## Correctness Properties

### Safety (Consistency)

- **Invariant**: All non-faulty replicas decide the same value for each slot
- **Mechanism**: Majority quorums + value recovery ensures only previously accepted or newly proposed values are chosen
- **Guarantee**: If value V₁ is decided in ballot B₁, then all future ballots B₂ > B₁ must also decide V₁ (for same slot)

### Liveness (Progress)

- **Condition**: Stable leader + reliable majority
- **Mechanism**: Phase 1 establishes supremacy; repeated Phase 2 with same ballot succeeds
- **Optimization**: Phase 1 executed once per term; Phase 2 can execute multiple times

### Fault Tolerance

- **Crashes Tolerated**: Up to ⌊(n-1)/2⌋ replicas may crash
- **Byzantine Tolerance**: NO (Paxos assumes honest replicas with crash faults only)
- **Requirements**:
  - Need n ≥ 3f + 1 where f = max failures tolerated
  - Majority = ⌈(n+1)/2⌉

---

## Execution Flow Example

### Scenario: 3-replica system, initial proposal

```
Client sends: ProposeRequest(instance=1, opId=UUID_A, op=[SET key=value])

REPLICA 0 (becomes proposer):
  1. beginPhase1() with Ballot(1, replica0)
  2. Send PrepareMessage(Ballot(1,0)) to replicas 1,2

REPLICAS 1,2 (acceptors):
  3. handlePrepareMessage: Update np=Ballot(1,0), reply with empty accepted values

REPLICA 0 (learner):
  4. handlePrepareReplyMessage: Collect 2 OK replies (majority), set phase1Complete=true
  5. acceptProposal: No recovered values, use proposed opId=UUID_A
  6. Send AcceptMessage(Ballot(1,0), instance=1, opId=UUID_A, op=[SET...]) to replicas 1,2

REPLICAS 1,2 (acceptors):
  7. handleAcceptMessage: Check Ballot(1,0) >= np=Ballot(1,0), accept!
     Set na=Ballot(1,0), va=opId_A, reply with AcceptReplyMessage

REPLICA 0 (learner):
  8. handleAcceptReplyMessage: Collect 2 OK replies (majority) for ballot(1,0)
  9. Decide! Send DecideMessage to all replicas
  10. triggerNotification(DecidedNotification) to upper layer (StateMachine)

ALL REPLICAS:
  11. handleDecideMessage: Mark instance 1 as decided, notify upper layer
```

---

## Architecture Integration

```
┌─────────────────────────────┐
│    Upper Layer (SMR/App)    │
│      StateMachine.java      │
└──────────┬──────────────────┘
           │ ProposeRequest
           │ StealLeaderRequest
           ↓
┌─────────────────────────────┐
│  MultiPaxosAgreement        │  ← YOU ARE HERE
│  (Consensus Protocol)       │
└──────────┬──────────────────┘
           │ DecidedNotification
           │ LeaderChangeNotification
           │
           ↓ TCP Messages
┌─────────────────────────────┐
│  Babel Framework            │
│  TCPChannel                 │
└──────────┬──────────────────┘
           │ Network
           ↓
    Other Replicas
```

---

## Configuration

### Required Properties

```properties
# Self address and port
babel.address=127.0.0.1
babel.port=5000

# Initial membership (comma-separated list)
initial_membership=127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002

# TCP Channel settings (auto-configured)
# HEARTBEAT_INTERVAL: 1000ms
# HEARTBEAT_TOLERANCE: 3000ms (timeout)
# CONNECT_TIMEOUT: 1000ms
```

---

## Performance Characteristics

| Metric            | Common Case  | Value Recovery | After Crash |
| ----------------- | ------------ | -------------- | ----------- |
| **Latency**       | 2 RTTs       | 2 RTTs         | 3 RTTs      |
| **Messages/Slot** | 4 (in + out) | 4              | 6           |
| **Phase 1 Count** | 1 per term   | 1 per term     | 1 per crash |

**Explanation**:

- **Common Case**: Phase 2 only (2 RTTs: ACCEPT→reply→DECIDED)
- **Value Recovery**: Still Phase 2 after Phase 1 (2 RTTs for Phase 2)
- **After Crash**: Phase 1 + Phase 2 (3 RTTs total)

---

## Testing & Debugging

### Logging Levels

Set in `log4j2.xml`:

```xml
<Logger name="protocols.agreement.MultiPaxosAgreement" level="DEBUG"/>
```

### Key Log Messages

```
// Leadership establishment
"Phase 1 complete - became leader with ballot X. Recovered values from majority."

// Value recovery
"Using recovered value for instance Y: opId=Z"

// Decision
"Instance X decided with ballot Y and opId: Z"

// Higher ballot seen (split-brain prevention)
"Higher ballot Y seen for instance X. Resetting accept votes."
```

---

## Known Limitations & Future Work

### Current Implementation

- ✅ Handles leader crashes
- ✅ Recovers previously accepted values
- ✅ Tolerates up to ⌊(n-1)/2⌋ crash faults
- ❌ No dynamic membership changes (AddReplica/RemoveReplica not implemented)

### Potential Enhancements

1. **Stable Storage**: Persist (na, va) to disk for crash recovery
2. **Dynamic Reconfiguration**: Support cluster membership changes
3. **Snapshots**: Compress decided slots into snapshots
4. **Batching**: Multiple operations per slot for throughput
5. **Pipelining**: Overlap Phase 1 and Phase 2 execution

---

## References

1. **Paxos Made Simple** - Leslie Lamport (2001)
2. **Paxos Made Practical** - Tushar Chandra, Robert Griesemer, Joshua Redstone
3. **Multi-Paxos Protocol** - Heidi Howard's thesis on consensus
4. **Babel Framework Documentation** - NOVA LINCS

---

## Summary

This implementation provides a **robust, correct Multi-Paxos consensus protocol** that:

1. ✅ **Follows formal specification** - Implements proposer, acceptor, learner algorithms exactly
2. ✅ **Recovers values** - Handles leader crashes via Phase 1 recovery
3. ✅ **Prevents split-brain** - Ballot tracking prevents multiple accepted values
4. ✅ **Integrates with Babel** - Uses proper message handlers and network layer
5. ✅ **Notifies upper layers** - Provides clear API via requests/notifications

The key insight: **Paxos is simple once you understand that it's all about ensuring no two different values are ever decided for the same slot, and ballot-based voting ensures this property.**
