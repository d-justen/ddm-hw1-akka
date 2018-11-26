package de.hpi.octopus;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import com.typesafe.config.Config;

import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Profiler;
import de.hpi.octopus.actors.Worker;
import de.hpi.octopus.actors.listeners.ClusterListener;
import scala.util.control.Exception.Catch;

public class OctopusMaster extends OctopusSystem {

	public static final String MASTER_ROLE = "master";
	public static int nrOfSlavesJoined = 0;

	public static void start(String actorSystemName, int workers, String host, int port, String filepath,
			int nrOfSlaves) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port);

		final ActorSystem system = createSystem(actorSystemName, config);

		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
				// system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

				system.actorOf(Profiler.props(), Profiler.DEFAULT_NAME);

				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);

				OctopusMaster.nrOfSlavesJoined++;
				System.out.println("nr joined " + OctopusMaster.nrOfSlavesJoined);

				if (nrOfSlaves == OctopusMaster.nrOfSlavesJoined) {
					long startTime = System.currentTimeMillis();
					String[][] input_csv = readCSV_by_column(filepath);
					if (input_csv != null) {
						system.actorSelection("/user/" + Profiler.DEFAULT_NAME)
								.tell(new Profiler.TaskMessage(input_csv, nrOfSlaves, startTime), ActorRef.noSender());
					}
				}
			}
		});

	}

	private static String[][] readCSV_by_column(String path) {
		File f = new File(path);
		if (!f.exists() || f.isDirectory()) {
			return null;
		}
		try {
			BufferedReader buffRe = new BufferedReader(new FileReader(path));
			String line = buffRe.readLine();
			ArrayList<String[]> table = new ArrayList<>();
			// boolean header = false; // not needed
			// we do not need the names, as long as the csv is ordered
			// ArrayList<String> name = new ArrayList<>();
			ArrayList<String> hash = new ArrayList<>();
			ArrayList<String> gene = new ArrayList<>();

			while ((line = buffRe.readLine()) != null) {
				// if (header == false) {
				if (!line.isEmpty()) {
					String[] row = line.split(";");
					// name.add(row[1]);
					hash.add(row[2]);
					gene.add(row[3]);
				}
				// header = false;
			}

			int len = hash.size();
			// name.toArray(new String[len])
			String[][] result = { hash.toArray(new String[len]), gene.toArray(new String[len]) };
			System.out.println("sucessfully read csv file " + path);
			return result;
		} catch (

		IOException ioe) {
			System.out.println("IOException thrown!");
			ioe.printStackTrace();
			return null;
		}
	}
}
