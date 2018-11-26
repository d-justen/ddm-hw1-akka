package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import de.hpi.octopus.actors.Profiler.RegistrationMessage;
import de.hpi.octopus.actors.listeners.ClusterListener;
import de.hpi.octopus.messages.ShutdownMessage;
import de.hpi.octopus.actors.Reaper;

import lombok.AllArgsConstructor;
import lombok.Data;
import akka.actor.PoisonPill;

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

	@Data
	@AllArgsConstructor
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 4545299661052078209L;
	}

	@Data
	@AllArgsConstructor
	@SuppressWarnings("unused")
	public static class TaskMessage implements Serializable {
		private static final long serialVersionUID = -8330958742629706627L;

		private TaskMessage() {
		}

		private String[][] columns;
		private int nrOfSlaves;
		private long startTime;
	}

	@Data
	@AllArgsConstructor
	@SuppressWarnings("unused")
	public static class PasswordCompletionMessage implements Serializable {
		private static final long serialVersionUID = -6823011111281387873L;

		private PasswordCompletionMessage() {
		}

		private String password, id;
	}

	@Data
	@AllArgsConstructor
	@SuppressWarnings("unused")
	public static class LinearCompletionMessage implements Serializable {
		private static final long serialVersionUID = 6823011111281389301L;

		private LinearCompletionMessage() {
		}

		private boolean solved;
		private int[] prefixes;
	}

	@Data
	@AllArgsConstructor
	@SuppressWarnings("unused")
	public static class GeneCompletionMessage implements Serializable {
		private static final long serialVersionUID = -35960706709105998L;

		private GeneCompletionMessage() {
		}

		private int partner1;
		private int partner2;
	}

	@Data
	@AllArgsConstructor
	@SuppressWarnings("unused")
	public static class HashMiningCompletionMessage implements Serializable {
		private static final long serialVersionUID = 4267335851792184144L;

		private HashMiningCompletionMessage() {
		}

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

	private int[] passwords;
	private int[] genePartners;
	private int[] prefixes;
	private int nrPasswords = 0;
	private int nrGenePartners = 0;
	private boolean hashMiningStarted = false;
	private long lastMax = 0;
	private boolean solved = false;
	private long startTime;
	private final Set<String> slaves = new HashSet<>();
	private int hashedPartners = 0;
	boolean task_stated = false;

	private TaskMessage task;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		/*
		 * return receiveBuilder().match(RegistrationMessage.class,
		 * this::handle).match(Terminated.class, this::handle) .match(TaskMessage.class,
		 * this::handle).match(PasswordCompletionMessage.class, this::handle)
		 * .match(LinearCompletionMessage.class,
		 * this::handle).match(GeneCompletionMessage.class, this::handle)
		 * .match(HashMiningCompletionMessage.class, this::handle) .matchAny(object ->
		 * this.log.info("Received unknown message: \"{}\"",
		 * object.toString())).build();
		 */
		return receiveBuilder().match(RegistrationMessage.class, this::handle).match(Terminated.class, this::handle)
				.match(TaskMessage.class, this::handle).match(PasswordCompletionMessage.class, this::handle)
				.match(LinearCompletionMessage.class, this::handle).match(GeneCompletionMessage.class, this::handle)
				.match(HashMiningCompletionMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString())).build();
	}

	private void startTask() {
		if (this.task_stated) {
			return;
		}
		this.task_stated = true;

		TaskMessage message = this.task;
		this.startTime = message.startTime;

		System.out.println("Profiler handling TaskMessage");
		if (this.task != null)
			this.log.error("The profiler actor can process only one task in its current implementation!");

		this.task = message;
		int len = message.columns[0].length;
		this.passwords = new int[len];
		this.genePartners = new int[len];
		Arrays.fill(this.genePartners, -1);
		this.prefixes = new int[len];

		for (int i = 0; i < len; i++) {
			this.assign(new Worker.PasswordMessage(message.columns[0][i], Integer.toString(i + 1)));
			this.assign(new Worker.GeneMessage(message.columns[1], i + 1));
		}
	}

	private void handle(PasswordCompletionMessage message) {
		ActorRef worker = this.sender();
		this.busyWorkers.remove(worker);
		this.passwords[Integer.parseInt(message.id) - 1] = Integer.parseInt(message.password);
		this.log.info("Hash nr {} is equal to password {}", message.id, message.password);
		this.nrPasswords++;

		// System.out.println(".length is " + Integer.toString(this.passwords.length) +
		// " nrPasswords is "
		// + Integer.toString(this.nrPasswords));

		if (this.passwords.length == this.nrPasswords) {
			System.out.println("Completed calculating all passwords!");
			assignLinear();
		} else {
			this.assign(worker);
		}
	}

	private void handle(LinearCompletionMessage message) {

		ActorRef worker = this.sender();
		this.busyWorkers.remove(worker);

		if (!message.solved) {
			this.log.info("failed to solve lin. comb. continue trying...");
		}

		if (solved || message.solved) {
			solved = true;

			if (!unassignedWork.isEmpty()) {
				unassignedWork.removeIf(o -> (o instanceof Worker.LinearCombinationMessage));
			}
		} else if (unassignedWork.isEmpty())
			assignLinear();

		if (message.solved && this.genePartners.length == this.nrGenePartners) {
			this.log.info("solved Linear Combination. Combination is:");
			this.log.info(Arrays.toString(message.prefixes));
			this.prefixes = message.prefixes;
			assignHashMining();
		} else {
			this.assign(worker);
		}
	}

	private void handle(GeneCompletionMessage message) {
		ActorRef worker = this.sender();
		this.busyWorkers.remove(worker);

		if (this.genePartners[message.partner1 - 1] == -1) {
			this.genePartners[message.partner1 - 1] = message.partner2 - 1;
			this.nrGenePartners++;
			this.log.info("gene nr {} is \'similar to\' gene nr {}", message.partner1, message.partner2);
			if (this.nrGenePartners % 5 == 0) {
				// print every 5 users how far along you are already
				this.log.info("Finished {} out of {} gine partners", this.nrGenePartners, this.genePartners.length);
			}
		} else {
			this.log.info("already found partner");
		}

		if (this.genePartners.length == this.nrGenePartners && this.solved) {
			assignHashMining();
		} else {
			this.assign(worker);
		}
	}

	private void handle(HashMiningCompletionMessage message) {
		ActorRef worker = this.sender();
		this.busyWorkers.remove(worker);

		this.log.info("HashMining: partner1: {}, partner2: {}, hash: {}", message.partner1, message.partner2,
				message.hash);
		this.hashedPartners++;
		this.assign(worker);

		if (this.hashedPartners == nrGenePartners) {
			this.log.info("All tasks completed in {} ms", System.currentTimeMillis() - startTime);

			if (!busyWorkers.isEmpty())
				busyWorkers.forEach((k, v) -> k.tell(PoisonPill.getInstance(), this.self()));
			idleWorkers.forEach(e -> e.tell(PoisonPill.getInstance(), this.self()));

			this.context().system().terminate();
		}
	}

	private void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.assign(this.sender());
		String hostPort = this.sender().path().address().hostPort();
		this.log.info("Registered {}, {}", this.sender(), hostPort);

		if (!hostPort.equals(this.self().path().address().hostPort()) && slaves.add(hostPort))
			log.info("Slave {} joined.", slaves.size());
		if (task != null && slaves.size() == task.nrOfSlaves)
			startTask();
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
		if (task.nrOfSlaves == slaves.size())
			startTask();
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
		for (int i = 0; i < 100; i++) {
			long newMin = lastMax + 10000000 * i;
			long newMax = lastMax + 10000000 * (i + 1);
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
	/*
	 * private void report(Worker.PasswordMessage work) { this.log.info("UCC: {}",
	 * work.getHash()); }
	 * 
	 * private void split(Worker.PasswordMessage work) {
	 * 
	 * String[] x = work.getHash();
	 * 
	 * int next = x.length + y.length;
	 * 
	 * if (next < this.task.getAttributes() - 1) { int[] xNew = Arrays.copyOf(x,
	 * x.length + 1); xNew[x.length] = next; this.assign(new WorkMessage(xNew, y));
	 * 
	 * int[] yNew = Arrays.copyOf(y, y.length + 1); yNew[y.length] = next;
	 * this.assign(new WorkMessage(x, yNew)); } }
	 */
}