package de.hpi.octopus;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Profiler;
import de.hpi.octopus.actors.Reaper;
import de.hpi.octopus.actors.Worker;
import de.hpi.octopus.actors.listeners.ClusterListener;

public class OctopusMaster extends OctopusSystem {
	
	public static final String MASTER_ROLE = "master";

	public static void start(String actorSystemName, int workers, String host, int port, String filepath, int nrSlaves) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port);
		
		final ActorSystem system = createSystem(actorSystemName, config);

        system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
        system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
        //	system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

        system.actorOf(Profiler.props(), Profiler.DEFAULT_NAME);

        for (int i = 0; i < workers; i++)
            system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);

        try {
			system.actorSelection("/user/" + Profiler.DEFAULT_NAME).tell(new Profiler.TaskMessage(readCSV(filepath), nrSlaves), ActorRef.noSender());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static String[][] readCSV(String path) throws FileNotFoundException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		ArrayList<String[]> table = new ArrayList<>();

		while ((line = br.readLine()) != null) {
			if (!line.isEmpty()) {
				String[] row = line.split(";");
				table.add(row);
			}
		}
		return table.toArray(new String[table.size()][]);
	}
}
