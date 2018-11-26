package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.listeners.ClusterListener;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Profiler extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "profiler";

	public static Props props() {
		return Props.create(Profiler.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @AllArgsConstructor
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 4545299661052078209L;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class TaskMessage implements Serializable {
		private static final long serialVersionUID = -8330958742629706627L;
		private TaskMessage() {}
		private String[][] table;
		private int nrSlaves;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class PasswordCompletionMessage implements Serializable {
		private static final long serialVersionUID = -6823011111281387873L;
		private PasswordCompletionMessage() {}
		private String password, id;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class LinearCompletionMessage implements Serializable {
		private static final long serialVersionUID = 6823011111281389301L;
		private LinearCompletionMessage() {}
		private boolean solved;
		private int[] prefixes;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class GeneCompletionMessage implements Serializable {
		private static final long serialVersionUID = -35960706709105998L;
		private GeneCompletionMessage() {}
		private int partner1;
		private int partner2;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class HashMiningCompletionMessage implements Serializable {
		private static final long serialVersionUID = 4267335851792184144L;
		private HashMiningCompletionMessage() {}
		private int partner1;
		private int partner2;
		private String hash;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	private final Queue<Object> unassignedWork = new LinkedList<>();
	private final Queue<ActorRef> idleWorkers = new LinkedList<>();
	private final Map<ActorRef, Object> busyWorkers = new HashMap<>();
	private final Set<String> slaves = new HashSet<>();

	private int[] passwords;
	private int[] genePartners;
	private int[] prefixes;
	private int nrPasswords = 0;
	private int nrGenePartners = 0;
	private boolean hashMiningStarted = false;
	private long lastMax = 0;
	private boolean solved = false;
	private long startTime;
	private int hashedPartners = 0;
	private boolean taskStarted = false;

	private TaskMessage task;

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RegistrationMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(TaskMessage.class, this::handle)
				.match(PasswordCompletionMessage.class, this::handle)
				.match(LinearCompletionMessage.class, this::handle)
				.match(GeneCompletionMessage.class, this::handle)
				.match(HashMiningCompletionMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	@Override
	public void preStart() throws Exception{
		super.preStart();
		Reaper.watchWithDefaultReaper(this);
	}

	private void startTask() {
		if (!taskStarted) {
			taskStarted = true;
			startTime = System.currentTimeMillis();
			this.passwords = new int[task.table.length];
			this.genePartners = new int[task.table.length];
			this.prefixes = new int[task.table.length];
			ArrayList<String> dna_seqs = new ArrayList<>();

			for (int i = 0; i < task.table.length; i++) {
				dna_seqs.add(task.table[i][3]);
			}

			for (int i = 0; i < task.table.length; i++) {
				this.assign(new Worker.PasswordMessage(task.table[i][2], task.table[i][0]));
				this.assign(new Worker.GeneMessage(i, dna_seqs));
			}
		}
	}

	private void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.assign(this.sender());
		String hostPort = this.sender().path().address().hostPort();
		this.log.info("Registered {}, {}", this.sender(), hostPort);

		if (!hostPort.equals(this.self().path().address().hostPort()) && slaves.add(hostPort))
			log.info("Slave {} joined.", slaves.size());
		if (task != null && slaves.size() == task.nrSlaves) startTask();
	}
	
	private void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		
		if (!this.idleWorkers.remove(message.getActor())) {
			Object work = this.busyWorkers.remove(message.getActor());
			if (work != null) {
				this.assign(work);
			}
		}		
		this.log.info("Unregistered {}", message.getActor());
	}

	private void handle(TaskMessage message) {
		if (this.task != null)
			this.log.error("The profiler actor can process only one task in its current implementation!");

		this.task = message;
		if (task.nrSlaves == slaves.size()) startTask();
	}
	
	private void handle(PasswordCompletionMessage message) {
		ActorRef worker = this.sender();
		this.busyWorkers.remove(worker);
		passwords[Integer.parseInt(message.id) - 1] = Integer.parseInt(message.password);
		this.log.info("Completed: [{},{}]", message.password, message.id);
		nrPasswords++;

		if (passwords.length == nrPasswords) assignLinear();

		this.assign(worker);
	}

	private void handle(LinearCompletionMessage message) {
		ActorRef worker = this.sender();
		this.busyWorkers.remove(worker);

		if (solved || message.solved) {
			solved = true;

			if (!unassignedWork.isEmpty()) {
				unassignedWork.removeIf(o -> (o instanceof Worker.LinearCombinationMessage));
			}
		} else if (unassignedWork.isEmpty()) assignLinear();

		if (message.solved && this.genePartners.length == this.nrGenePartners) {
			this.prefixes = message.prefixes;
			assignHashMining();
		}

		this.log.info("Completed: [{}]", message.solved);
		this.assign(worker);
	}

	private void handle(GeneCompletionMessage message) {
		ActorRef worker = this.sender();
		this.busyWorkers.remove(worker);

		this.genePartners[message.partner1] = message.partner2;
		this.nrGenePartners++;

		this.log.info("Completed: [{},{}]", message.partner1, message.partner2);
		this.assign(worker);

		if (this.genePartners.length == this.nrGenePartners && this.solved) assignHashMining();
	}

	private void handle(HashMiningCompletionMessage message) {
		ActorRef worker = this.sender();
		this.busyWorkers.remove(worker);

		this.log.info("Completed: [{},{},{}]", message.partner1, message.partner2, message.hash);
		hashedPartners++;
		this.assign(worker);

		if (hashedPartners == nrGenePartners) {
			this.log.info("All tasks completed in {} ms", System.currentTimeMillis() - startTime);

			if (!busyWorkers.isEmpty())
				busyWorkers.forEach((k, v) -> k.tell(PoisonPill.getInstance(), this.self()));
			idleWorkers.forEach(e -> e.tell(PoisonPill.getInstance(), this.self()));

			this.getContext().getSystem().actorSelection("/user/" + ClusterListener.DEFAULT_NAME).tell(PoisonPill.getInstance(), this.self());
			this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
		}
	}

	private void assign(Object work) {
		ActorRef worker = this.idleWorkers.poll();
		
		if (worker == null) {
			this.unassignedWork.add(work);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
	
	private void assign(ActorRef worker) {
		Object work = this.unassignedWork.poll();

		if (work == null) {
			this.idleWorkers.add(worker);
			return;
		}

		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}

	private void assignLinear() {

		for (int i=0; i<100; i++) {
			long newMin = lastMax + 10000000 * i;
			long newMax = lastMax + 10000000 * (i+1);
			assign(new Worker.LinearCombinationMessage(newMin, newMax, passwords));
		}
		lastMax += 1000000000;
	}

	private void assignHashMining() {

		if (this.hashMiningStarted) {
			return;
		}

		this.hashMiningStarted = true;

		for (int i = 0; i < this.genePartners.length; i++) {
			this.assign(new Worker.HashMiningMessage(i, this.genePartners[i], this.prefixes[i], 5));
		}

		this.log.info("Start hash mining");
	}
}