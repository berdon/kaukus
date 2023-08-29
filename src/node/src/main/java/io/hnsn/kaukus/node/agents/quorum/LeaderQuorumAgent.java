package io.hnsn.kaukus.node.agents.quorum;

import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.StateMachineConfig;
import com.github.oxo42.stateless4j.transitions.Transition;
import com.github.oxo42.stateless4j.triggers.TriggerWithParameters1;
import com.google.inject.Provider;
import io.hnsn.kaukus.encoding.LeaderAnnouncementMessage;
import io.hnsn.kaukus.encoding.LeaderElectedBroadcast;
import io.hnsn.kaukus.encoding.LeaderVoteBroadcast;
import io.hnsn.kaukus.encoding.RequestLeaderVoteBroadcast;
import io.hnsn.kaukus.encoding.RequestStorageDeleteMessage;
import io.hnsn.kaukus.encoding.RequestStorageSetMessage;
import io.hnsn.kaukus.encoding.StorageDeleteMessage;
import io.hnsn.kaukus.encoding.StorageSetMessage;
import io.hnsn.kaukus.node.agents.AgentException;
import io.hnsn.kaukus.node.agents.broadcast.BroadcastAgent;
import io.hnsn.kaukus.node.agents.connection.Connection;
import io.hnsn.kaukus.node.agents.connection.ConnectionAgent;
import io.hnsn.kaukus.node.agents.connection.OnMessageReceivedListener;
import io.hnsn.kaukus.node.agents.storage.StorageAgent;
import io.hnsn.kaukus.node.state.ListenerSource;
import io.hnsn.kaukus.types.Namespace;
import io.hnsn.kaukus.utilities.Ref;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.slf4j.Logger;

@RequiredArgsConstructor
public class LeaderQuorumAgent implements QuorumAgent {
  private final ConnectionAgent connectionAgent;
  private final BroadcastAgent broadcastAgent;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Logger log;
  private final ListenerSource listeners = new ListenerSource();
  private final Provider<String> nodeIdentifier;
  private final StorageAgent storageAgent;
  private final ExecutorService serialExecution = Executors.newSingleThreadExecutor();
  private String leaderNodeIdentifier = null;
  private StateMachine<LeaderQuorumState, LeaderQuorumTrigger> stateMachine;
  private ScheduledFuture<?> electionTimeout;

  @Override
  public void start() throws AgentException {
    connectionAgent.registerOnConnectionEstablishedListener(this::onConnectionEstablished);
    connectionAgent.registerOnConnectionClosedListener(this::onConnectionClosed);
    // Need to set up already established connections
    connectionAgent.getConnectedNodeIdentifiers().stream().map(connectionAgent::getConnection).forEach(this::onConnectionEstablished);
    broadcastAgent.registerOnLeaderRequested(b -> serialExecution.submit(() -> {
      if (stateMachine.getState() == LeaderQuorumState.Leader) {
        connectionAgent.getConnection(b.getNodeIdentifier()).sendMessage(
            new LeaderAnnouncementMessage(leaderNodeIdentifier, nodeIdentifier.get()));
      }
      else {
        stateMachine.fire(LeaderQuorumTrigger.LeaderRequested);
      }
    }));
    broadcastAgent.registerOnLeaderVoteRequested(b -> serialExecution.submit(() -> stateMachine.fire(triggerVoteRequested, b)));
    broadcastAgent.registerOnLeaderVote(b -> serialExecution.submit(() -> stateMachine.fire(triggerVoteReceived, b)));
    broadcastAgent.registerOnLeaderElected(b -> serialExecution.submit(() -> stateMachine.fire(triggerLeaderElected, b.getNodeIdentifier())));
    stateMachine = new StateMachine<>(LeaderQuorumState.Leaderless, new LeaderQuorumStateMachineConfig());
  }

  private void onConnectionEstablished(Connection connection) {
    connection.registerOnMessageReceivedListener(this::onConnectionMessageReceived);
    serialExecution.submit(() -> stateMachine.fire(LeaderQuorumTrigger.NewConnection));
  }

  private void onConnectionClosed(Connection connection) {
    if (connection.getNodeIdentifier().equals(leaderNodeIdentifier)) {
      serialExecution.submit(() -> stateMachine.fire(LeaderQuorumTrigger.LeaderDisconnected));
    }
  }

  private void onConnectionMessageReceived(OnMessageReceivedListener listener, GenericRecord payload) {
    if (payload.getSchema().getName().compareTo(LeaderAnnouncementMessage.SCHEMA$.getName()) == 0) {
      final var leaderAnnouncementMessage = (LeaderAnnouncementMessage) SpecificData.getForSchema(LeaderAnnouncementMessage.SCHEMA$).deepCopy(payload.getSchema(), payload);
      final var newLeaderNodeIdentifier = leaderAnnouncementMessage.getLeaderNodeIdentifier();
      serialExecution.submit(() -> stateMachine.fire(triggerLeaderAnnounced, newLeaderNodeIdentifier));
    }
    else if (payload.getSchema().getName().compareTo(StorageSetMessage.SCHEMA$.getName()) == 0) {
      final var message = (StorageSetMessage) SpecificData.getForSchema(StorageSetMessage.SCHEMA$).deepCopy(payload.getSchema(), payload);
      final var namespace = new Ref<Namespace>();
      if (!Namespace.tryParse(message.getNamespace(), namespace)) {
        log.error("Failed to parse storage set's namespace {}.", message.getNamespace());
        return;
      }

      log.info("Handling set announcement for \"{}/{}\".", namespace.getValue(), message.getKey());
      storageAgent.set(namespace.getValue(), message.getKey(), message.getValue());
    }
    else if (payload.getSchema().getName().compareTo(RequestStorageSetMessage.SCHEMA$.getName()) == 0) {
      final var message = (RequestStorageSetMessage) SpecificData.getForSchema(RequestStorageSetMessage.SCHEMA$).deepCopy(payload.getSchema(), payload);
      final var namespace = new Ref<Namespace>();
      if (!Namespace.tryParse(message.getNamespace(), namespace)) {
        log.error("Failed to parse request storage set's namespace {}.", message.getNamespace());
        return;
      }

      requestStorageSet(namespace.getValue(), message.getKey(), message.getValue());
    }
    else if (payload.getSchema().getName().compareTo(StorageDeleteMessage.SCHEMA$.getName()) == 0) {
      final var message = (StorageDeleteMessage) SpecificData.getForSchema(StorageDeleteMessage.SCHEMA$).deepCopy(payload.getSchema(), payload);
      final var namespace = new Ref<Namespace>();
      if (!Namespace.tryParse(message.getNamespace(), namespace)) {
        log.error("Failed to parse storage delete's namespace {}.", message.getNamespace());
        return;
      }

      log.info("Handling delete announcement for \"{}/{}\".", namespace.getValue(), message.getKey());
      storageAgent.delete(namespace.getValue(), message.getKey());
    }
    else if (payload.getSchema().getName().compareTo(RequestStorageDeleteMessage.SCHEMA$.getName()) == 0) {
      final var message = (RequestStorageDeleteMessage) SpecificData.getForSchema(RequestStorageDeleteMessage.SCHEMA$).deepCopy(payload.getSchema(), payload);
      final var namespace = new Ref<Namespace>();
      if (!Namespace.tryParse(message.getNamespace(), namespace)) {
        log.error("Failed to parse request storage delete's namespace {}.", message.getNamespace());
        return;
      }

      requestStorageDelete(namespace.getValue(), message.getKey());
    }
  }

  @Override
  public void close() throws IOException {
    if (electionTimeout != null) {
      electionTimeout.cancel(true);
    }
    serialExecution.shutdown();
  }

  @Override
  public String getLeaderNodeIdentifier() {
    return leaderNodeIdentifier;
  }

  @Override
  public boolean isLeader() {
    return nodeIdentifier.get().equals(leaderNodeIdentifier);
  }

  @Override
  public void requestStorageSet(Namespace namespace, String key, String value) {
    if (isLeader()) {
      log.info("Set requested for \"{}/{}\".", namespace.toString(), key);
      // No need to request - just set and broadcast
      storageAgent.set(namespace, key, value);
      log.debug("Broadcasting set announcement for \"{}/{}\".", namespace.toString(), key);
      connectionAgent.getConnectedNodeIdentifiers().forEach(id -> {
        final var connection = connectionAgent.getConnection(id);
        connection.sendMessage(new StorageSetMessage(namespace.toString(), key, value, nodeIdentifier.get()));
      });
    }
    else {
      log.info("Set request proxied to leader [{}] for \"{}/{}\".", leaderNodeIdentifier, namespace.toString(), key);
      connectionAgent.getConnection(leaderNodeIdentifier).sendMessage(new RequestStorageSetMessage(
          namespace.toString(), key, value, nodeIdentifier.get()));
    }
  }

  @Override
  public void requestStorageDelete(Namespace namespace, String key) {
    if (isLeader()) {
      log.info("Delete requested for \"{}/{}\".", namespace.toString(), key);
      // No need to request - just set and broadcast
      storageAgent.delete(namespace, key);
      log.debug("Broadcasting delete announcement for \"{}/{}\".", namespace, key);
      connectionAgent.getConnectedNodeIdentifiers().forEach(id -> {
        final var connection = connectionAgent.getConnection(id);
        connection.sendMessage(new StorageDeleteMessage(namespace.toString(), key, nodeIdentifier.get()));
      });
    }
    else {
      log.info("Delete request proxied to leader [{}] for \"{}/{}\".", leaderNodeIdentifier, namespace.toString(), key);
      connectionAgent.getConnection(leaderNodeIdentifier).sendMessage(new RequestStorageDeleteMessage(
          namespace.toString(), key, nodeIdentifier.get()));
    }
  }

  @Override
  public void registerOnLeaderChangedListener(OnLeaderChangedListener listener) {
    listeners.get(OnLeaderChangedListener.class).add(listener);
  }

  @Override
  public void unregisterOnLeaderChangedListener(OnLeaderChangedListener listener) {
    listeners.get(OnLeaderChangedListener.class).remove(listener);
  }

  private TriggerWithParameters1<LeaderElectedBroadcast, LeaderQuorumTrigger> triggerElectionResultsReceived;
  private TriggerWithParameters1<LeaderVoteBroadcast, LeaderQuorumTrigger> triggerVoteReceived;
  private TriggerWithParameters1<RequestLeaderVoteBroadcast, LeaderQuorumTrigger> triggerVoteRequested;
  private TriggerWithParameters1<String, LeaderQuorumTrigger> triggerLeaderAnnounced;
  private TriggerWithParameters1<String, LeaderQuorumTrigger> triggerLeaderElected;

  private class LeaderQuorumStateMachineConfig extends StateMachineConfig<LeaderQuorumState, LeaderQuorumTrigger> {
    private final Random random = new Random();
    private Integer epoch = 0;
    private final Map<Integer, Map<String, Integer>> votes = new HashMap<>();
    private final Map<Integer, Set<String>> voteParticipants = new HashMap<>();
    private final Map<Integer, List<LeaderElectedBroadcast>> electionResults = new HashMap<>();

    public LeaderQuorumStateMachineConfig()
    {
      triggerLeaderAnnounced = setTriggerParameters(LeaderQuorumTrigger.LeaderAnnounced, String.class);
      triggerVoteRequested = setTriggerParameters(LeaderQuorumTrigger.VoteRequested, RequestLeaderVoteBroadcast.class);
      triggerVoteReceived = setTriggerParameters(LeaderQuorumTrigger.VoteReceived, LeaderVoteBroadcast.class);
      triggerElectionResultsReceived = setTriggerParameters(LeaderQuorumTrigger.ElectionResultsReceived, LeaderElectedBroadcast.class);
      triggerLeaderElected = setTriggerParameters(LeaderQuorumTrigger.LeaderElected, String.class);

      configure(LeaderQuorumState.Leaderless)
          .onEntry((t, a) -> log.info("{} -> {}", t.getSource(), t.getDestination()))
          .onEntry(broadcastAgent::broadcastRequestLeader)
          .permit(LeaderQuorumTrigger.LeaderAnnounced, LeaderQuorumState.Leader)
          .permit(LeaderQuorumTrigger.NewConnection, LeaderQuorumState.RequestingLeader, broadcastAgent::broadcastRequestLeader)
          .permit(LeaderQuorumTrigger.VoteRequested, LeaderQuorumState.StartingVote, this::requestLeaderVote)
          .permit(LeaderQuorumTrigger.LeaderRequested, LeaderQuorumState.StartingVote, this::requestLeaderVote);

      configure(LeaderQuorumState.Leader)
          .onEntry((t, a) -> log.info("{} -> {}", t.getSource(), t.getDestination()))
          .onEntry((t, a) -> leaderNodeIdentifier = (String) a[0])
          .permitReentry(LeaderQuorumTrigger.LeaderNominated,
              () -> log.info("Previous leader handed leadership over to {}.", leaderNodeIdentifier))
          .permit(LeaderQuorumTrigger.ElectionFailure, LeaderQuorumState.StartingVote, this::requestLeaderVote)
          .permitDynamic(triggerElectionResultsReceived, broadcast -> {
            if (!broadcast.getNodeIdentifier().equals(leaderNodeIdentifier)) {
              requestLeaderVote();
              return LeaderQuorumState.StartingVote;
            }

            return LeaderQuorumState.Leader;
          })
          .permit(LeaderQuorumTrigger.LeaderDisconnected, LeaderQuorumState.StartingVote, this::requestLeaderVote);

      configure(LeaderQuorumState.RequestingLeader)
          .onEntry((t, a) -> log.info("{} -> {}", t.getSource(), t.getDestination()))
          .permit(LeaderQuorumTrigger.VoteRequested, LeaderQuorumState.StartingVote, this::requestLeaderVote)
          .permit(LeaderQuorumTrigger.LeaderRequested, LeaderQuorumState.StartingVote, this::requestLeaderVote)
          .permitReentry(LeaderQuorumTrigger.NewConnection, broadcastAgent::broadcastRequestLeader)
          .permit(LeaderQuorumTrigger.LeaderAnnounced, LeaderQuorumState.Leader);

      configure(LeaderQuorumState.StartingVote)
          .onEntry((t, a) -> log.info("{} -> {}", t.getSource(), t.getDestination()))
          .onEntry((t, a) -> {
            if (a != null && a.length > 0) {
              if (t.getTrigger() == LeaderQuorumTrigger.VoteRequested
                  && a[0] instanceof RequestLeaderVoteBroadcast leaderRequest) {
                // Take any new epoch
                if (leaderRequest.getEpoch() > epoch) {
                  log.info("Requested vote had a higher epoch ({} > {}); upgrading epoch.", leaderRequest.getEpoch(), epoch);
                  epoch = leaderRequest.getEpoch();

                  // Cancel any election timeout
                  if (electionTimeout != null) {
                    electionTimeout.cancel(true);
                    electionTimeout = null;
                  }
                }
              }
            }

            log.info("Starting vote for epoch {}.", epoch);
            if (electionTimeout == null) {
              final var currentEpoch = epoch;
              electionTimeout = scheduledExecutorService.schedule(() -> {
                if (currentEpoch.equals(epoch) && (
                    stateMachine.getState() == LeaderQuorumState.Election
                        || stateMachine.getState() == LeaderQuorumState.Tallying)) {
                  log.info("Voting failed; reverting to leaderless.");
                  serialExecution.submit(() -> stateMachine.fire(LeaderQuorumTrigger.ElectionFailure));
                }

                electionTimeout = null;
              }, 10, TimeUnit.SECONDS);
            }

            // Clear the current leader
            leaderNodeIdentifier = null;

            // Track vote participants
            final var participants = voteParticipants.computeIfAbsent(epoch, e -> new HashSet<>());
            participants.addAll(connectionAgent.getConnectedNodeIdentifiers());
            participants.add(nodeIdentifier.get());

            // Broadcast the vote
            final var vote = random.nextInt();
            votes.computeIfAbsent(epoch, e -> new HashMap<>()).put(nodeIdentifier.get(), vote);
            broadcastAgent.broadcastLeaderVote(vote, epoch);

            // Transition to election
            stateMachine.fire(LeaderQuorumTrigger.Voted);
          })
          .permitReentry(LeaderQuorumTrigger.NewConnection, this::requestLeaderVote)
          .permit(LeaderQuorumTrigger.Voted, LeaderQuorumState.Election)
          .permitReentry(LeaderQuorumTrigger.VoteRequested, this::requestLeaderVote)
          .permitReentry(LeaderQuorumTrigger.LeaderRequested, this::requestLeaderVote);

      configure(LeaderQuorumState.Election)
          .onEntry((t, a) -> log.info("{} -> {}", t.getSource(), t.getDestination()))
          .onEntry(this::onElectionResultsReceived)
          .onEntry((t, action) -> {
            if (action != null && action.length > 0
                && t.getTrigger() == LeaderQuorumTrigger.VoteReceived
                && action[0] instanceof LeaderVoteBroadcast leaderVote) {
              if (!connectionAgent.hasConnection(leaderVote.getNodeIdentifier())) {
                log.warn("Received vote from unknown connection [{}]; ignoring.", leaderVote.getNodeIdentifier());
                return;
              }

              // Track vote
              votes.computeIfAbsent(leaderVote.getEpoch(), e -> new HashMap<>())
                  .put(leaderVote.getNodeIdentifier(), leaderVote.getVote());

              if (votes.get(epoch).keySet().equals(voteParticipants.get(epoch))) {
                stateMachine.fire(LeaderQuorumTrigger.AllVotesReceived);
              }
            }
          })
          .permitReentry(LeaderQuorumTrigger.NewConnection, this::requestLeaderVote)
          .permitReentry(LeaderQuorumTrigger.VoteReceived)
          .permitReentry(LeaderQuorumTrigger.ElectionResultsReceived)
          .permit(LeaderQuorumTrigger.AllVotesReceived, LeaderQuorumState.Tallying)
          .permitDynamic(triggerVoteRequested, (t) -> t.getEpoch() > epoch
              ? LeaderQuorumState.StartingVote
              : LeaderQuorumState.Election)
          .permit(LeaderQuorumTrigger.LeaderRequested, LeaderQuorumState.StartingVote, this::requestLeaderVote)
          .permit(LeaderQuorumTrigger.ElectionFailure, LeaderQuorumState.Leaderless)
          .permit(LeaderQuorumTrigger.LeaderAnnounced, LeaderQuorumState.StartingVote, this::requestLeaderVote);

      configure(LeaderQuorumState.Tallying)
          .onEntry((t, a) -> log.info("{} -> {}", t.getSource(), t.getDestination()))
          .onEntry(this::onElectionResultsReceived)
          .onEntry((t, action) -> {
            final var votes = this.votes.get(epoch);
            final var winner = votes.entrySet().stream().max(Comparator.comparingInt(Entry::getValue));
            if (winner.isEmpty()) {
              stateMachine.fire(LeaderQuorumTrigger.ElectionFailure);
              return;
            }

            votes.forEach((id, vote) -> log.info("{} -> {}", id, vote));

            int count = 0;
            for (var value : votes.values()) {
              if (value.equals(winner.get().getValue())) {
                count++;
                if (count > 1) {
                  requestLeaderVote();
                  stateMachine.fire(LeaderQuorumTrigger.VoteRequested);
                  return;
                }
              }
            }

            log.info("Winning leader: {}", winner.get().getKey());

            // We received election results - see if we matched
            leaderNodeIdentifier = winner.get().getKey();
            if (!leaderNodeIdentifier.equals(nodeIdentifier.get())) {
              log.info("Connection {} won the election.", leaderNodeIdentifier);
            }

            if (electionResults.containsKey(epoch)) {
              final var electionResults = this.electionResults.get(epoch);
              if (!electionResults.stream().allMatch(s -> s.getNodeIdentifier().equals(leaderNodeIdentifier))) {
                log.error("Local election result tally didn't match leader elected notice; requesting new election.");
                stateMachine.fire(LeaderQuorumTrigger.ElectionFailure);
                return;
              }
            }

            // Broadcast if we were the winner
            if (nodeIdentifier.get().equals(winner.get().getKey())) {
              log.info("Won leader election; broadcasting election results.");
              broadcastAgent.broadcastLeaderElected(epoch);
              stateMachine.fire(triggerLeaderElected, leaderNodeIdentifier);
            }
          })
          .permit(LeaderQuorumTrigger.NewConnection, LeaderQuorumState.StartingVote, this::requestLeaderVote)
          .permit(LeaderQuorumTrigger.LeaderElected, LeaderQuorumState.Leader)
          .permit(LeaderQuorumTrigger.ElectionFailure, LeaderQuorumState.Leaderless)
          .permit(LeaderQuorumTrigger.TallyingTimedOut, LeaderQuorumState.StartingVote, this::requestLeaderVote)
          .permitReentry(LeaderQuorumTrigger.ElectionResultsReceived);
    }


    private void onElectionResultsReceived(Transition<LeaderQuorumState, LeaderQuorumTrigger> t, Object[] action) {
      if (action != null && action.length > 0
          && t.getTrigger() == LeaderQuorumTrigger.LeaderAnnounced
          && action[0] instanceof LeaderElectedBroadcast broadcast) {
        electionResults.computeIfAbsent(broadcast.getEpoch(), e -> new ArrayList<>()).add(broadcast);
      }
    }

    private void requestLeaderVote() {
      epoch += 1;
      log.info("Broadcasting request leader vote for epoch {}.", epoch);
      broadcastAgent.broadcastRequestLeaderVote(epoch);
    }
  };

  private enum LeaderQuorumState {
    Leaderless,
    RequestingLeader,
    StartingVote,
    Election,
    Tallying,
    Leader
  }

  private enum LeaderQuorumTrigger {
    LeaderAnnounced,
    NewConnection,
    VoteRequested,
    LeaderRequested,
    Voted,
    ElectionFailure,
    VoteReceived,
    AllVotesReceived,
    LeaderElected,
    LeaderNominated,
    LeaderDisconnected,
    ElectionResultsReceived,
    TallyingTimedOut;
  }
}
