package de.hpi.octopus.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Random;
import java.util.Arrays;
import java.util.Collections;
import java.util.ArrayList;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.cluster.ClusterEvent.MemberUp;
import akka.remote.EndpointManager;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.Profiler.RegistrationMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Worker extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "worker";
	private static List<Integer> sign_sequence = new ArrayList<>();

	public static Props props() {
		return Props.create(Worker.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	@AllArgsConstructor
	@SuppressWarnings("unused")
	public static class PasswordMessage implements Serializable {
		private static final long serialVersionUID = -7643194361868862396L;

		private PasswordMessage() {
		}

		private String pwd_hash;
		private String id;
	}

	@Data
	@AllArgsConstructor
	@SuppressWarnings("unused")
	public static class LinearCombinationMessage implements Serializable {
		private static final long serialVersionUID = -7643194362638862396L;

		private LinearCombinationMessage() {
		}

		private long min, max;
		private int[] passwords;
	}

	@Data
	@AllArgsConstructor
	@SuppressWarnings("unused")
	public static class GeneMessage implements Serializable {
		private static final long serialVersionUID = -7684072222482718287L;

		private GeneMessage() {
		}

		private String[] dna_seqs;
		private int index;
	}

	@Data
	@AllArgsConstructor
	@SuppressWarnings("unused")
	public static class HashMiningMessage implements Serializable {
		private static final long serialVersionUID = -1351524675941721227L;

		private HashMiningMessage() {
		}

		private int index;
		private int partner;
		private int prefix;
		private int prefixLength;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
	private final Cluster cluster = Cluster.get(this.context().system());

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		this.cluster.subscribe(this.self(), MemberUp.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(CurrentClusterState.class, this::handle).match(MemberUp.class, this::handle)
				.match(PasswordMessage.class, this::handle).match(LinearCombinationMessage.class, this::handle)
				.match(GeneMessage.class, this::handle).match(HashMiningMessage.class, this::handle)
				.match(MemberUp.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString())).build();
	}

	private void register(Member member) {
		if (member.hasRole(OctopusMaster.MASTER_ROLE))
			this.getContext().actorSelection(member.address() + "/user/" + Profiler.DEFAULT_NAME)
					.tell(new RegistrationMessage(), this.self());
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void handle(PasswordMessage message) throws UnsupportedEncodingException, NoSuchAlgorithmException {
	    for (int i=100000; i<1000000; i++) {
	        String hashed = hash(Integer.toString(i));
	        if (hashed.equals(message.pwd_hash.toLowerCase())) {
	            this.sender().tell(new Profiler.PasswordCompletionMessage(Integer.toString(i), message.id), this.self());
	            return;
            }
        }
        this.sender().tell(new Profiler.PasswordCompletionMessage("", "-1"), this.self());
	}
	
	
	private void handle(LinearCombinationMessage message) {
		int[] prefixes = new int[message.passwords.length];

		for (long i = message.min; i < message.max; i++) {
			String bin = Long.toBinaryString(i);
			for (int j = 0; j < prefixes.length; j++)
				prefixes[j] = 1;

			int count = 0;
			for (int k = bin.length() - 1; k >= 0; k--) {
				if (bin.charAt(k) == '1')
					prefixes[count] = -1;
				count++;
			}
			int sum = 0;
			for (int l = 0; l < message.passwords.length; l++) {
				sum += prefixes[l] * message.passwords[l];
			}
			if (sum == 0) {
				this.sender().tell(new Profiler.LinearCompletionMessage(true, prefixes), this.self());
				return;
			}
		}
		this.sender().tell(new Profiler.LinearCompletionMessage(false, null), this.self());
	}

	private void handle(GeneMessage message) {
		int partner = longestOverlapPartner(message.index, message.dna_seqs);

		this.sender().tell(new Profiler.GeneCompletionMessage(message.index, partner), this.self());
	}

	private void handle(HashMiningMessage message) throws UnsupportedEncodingException, NoSuchAlgorithmException {
		String prefix = (message.prefix > 0) ? "1" : "0";

		String hash = this.findHash(message.partner, prefix, message.prefixLength);

		this.sender().tell(new Profiler.HashMiningCompletionMessage(message.index, message.partner, hash), this.self());
	}

	private String hash(String password) throws UnsupportedEncodingException, NoSuchAlgorithmException {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		byte[] hashedBytes = digest.digest(password.getBytes("UTF-8"));
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < hashedBytes.length; i++) {
			stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return stringBuffer.toString();
	}

	private String findHash(int content, String prefix, int prefixLength)
			throws UnsupportedEncodingException, NoSuchAlgorithmException {
		StringBuilder fullPrefixBuilder = new StringBuilder();
		for (int i = 0; i < prefixLength; i++)
			fullPrefixBuilder.append(prefix);

		Random rand = new Random(13);

		String fullPrefix = fullPrefixBuilder.toString();
		int nonce = 0;
		while (true) {
			nonce = rand.nextInt();
			String hash = this.hash("" + (content + nonce));
			if (hash.startsWith(fullPrefix))
				return hash;
		}
	}

	private int longestOverlapPartner(int thisIndex, String[] sequences) {
		thisIndex--; // index in csv starts at 1, not at 0
		int bestOtherIndex = -1;
		String bestOverlap = "";
		for (int otherIndex = 0; otherIndex < sequences.length; otherIndex++) {
			if (otherIndex == thisIndex)
				continue;

			String longestOverlap = this.longestOverlap(sequences[thisIndex], sequences[otherIndex]);

			if (bestOverlap.length() < longestOverlap.length()) {
				bestOverlap = longestOverlap;
				bestOtherIndex = otherIndex;
			}
		}
		return bestOtherIndex + 1; // conert from 0 indexed to 1 indexed
	}

	private String longestOverlap(String str1, String str2) {
		if (str1.isEmpty() || str2.isEmpty())
			return "";

		if (str1.length() > str2.length()) {
			String temp = str1;
			str1 = str2;
			str2 = temp;
		}

		int[] currentRow = new int[str1.length()];
		int[] lastRow = str2.length() > 1 ? new int[str1.length()] : null;
		int longestSubstringLength = 0;
		int longestSubstringStart = 0;

		for (int str2Index = 0; str2Index < str2.length(); str2Index++) {
			char str2Char = str2.charAt(str2Index);
			for (int str1Index = 0; str1Index < str1.length(); str1Index++) {
				int newLength;
				if (str1.charAt(str1Index) == str2Char) {
					newLength = str1Index == 0 || str2Index == 0 ? 1 : lastRow[str1Index - 1] + 1;

					if (newLength > longestSubstringLength) {
						longestSubstringLength = newLength;
						longestSubstringStart = str1Index - (newLength - 1);
					}
				} else {
					newLength = 0;
				}
				currentRow[str1Index] = newLength;
			}
			int[] temp = currentRow;
			currentRow = lastRow;
			lastRow = temp;
		}
		return str1.substring(longestSubstringStart, longestSubstringStart + longestSubstringLength);
	}
}