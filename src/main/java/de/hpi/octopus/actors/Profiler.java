package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
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
	
	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	private final Queue<Object> unassignedWork = new LinkedList<>();
	private final Queue<ActorRef> idleWorkers = new LinkedList<>();
	private final Map<ActorRef, Object> busyWorkers = new HashMap<>();

	private int[] passwords;
	private int[] genePartners;
	private int nrPasswords = 0;
	private int nrGenePartners = 0;
	private boolean hashMiningStarted = false;
	private long lastMax = 0;
	private boolean solved = false;

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
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		
		this.assign(this.sender());
		this.log.info("Registered {}", this.sender());
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
		this.passwords = new int[message.table.length];
		this.genePartners = new int[message.table.length];
		ArrayList<String> dna_seqs = new ArrayList<>();

		for (int i=0; i<message.table.length; i++) {
			dna_seqs.add(message.table[i][3]);
		}

		for (int i=0; i<message.table.length; i++) {
			this.assign(new Worker.PasswordMessage(message.table[i][2], message.table[i][0]));
			this.assign(new Worker.GeneMessage(i, dna_seqs));
		}
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

			if (this.genePartners.length == this.nrGenePartners) assignHashMining();
		} else if (unassignedWork.isEmpty()) assignLinear();

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

		this.log.info("Start hash mining");
	}
	
	private void report(Worker.PasswordMessage work) {
		//this.log.info("UCC: {}", work.getHash());
	}

	private void split(Worker.PasswordMessage work) {
		/*String[] x = work.getHash();

		int next = x.length + y.length;
		
		if (next < this.task.getAttributes() - 1) {
			int[] xNew = Arrays.copyOf(x, x.length + 1);
			xNew[x.length] = next;
			this.assign(new WorkMessage(xNew, y));
			
			int[] yNew = Arrays.copyOf(y, y.length + 1);
			yNew[y.length] = next;
			this.assign(new WorkMessage(x, yNew));
		}*/
	}
}