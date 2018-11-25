package de.hpi.octopus;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Profiler;
import de.hpi.octopus.actors.Worker;
import de.hpi.octopus.actors.listeners.ClusterListener;
import scala.util.control.Exception.Catch;

public class OctopusMaster extends OctopusSystem {

	public static final String MASTER_ROLE = "master";

	public static void start(String actorSystemName, int workers, String host, int port, String filepath) {

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
			}
		});

		boolean exit = false;
		String s;

		while (!exit) {
			System.out.println(
					"\nplease select what to calcultate.\nOptions are \'password\',\'gene\', \'linear\', \'hash\' and \'exit\''");

			Scanner scan = new Scanner(System.in);
			s = scan.nextLine();
			if (s == "exit") {
				// exit program
				exit = true;
			}
			if (s == "password" || s == "gene" || s == "linear" || s == "hash") {
				// actuall do stuff
				String[][] input_csv;
				switch (s) {
				case "password":
					input_csv = readCSV_by_column(filepath);
					if (input_csv != null) {
						system.actorSelection("/user/" + Profiler.DEFAULT_NAME)
								.tell(new Profiler.TaskMessage(input_csv), ActorRef.noSender());
					}
					break;
				case "gene":
					input_csv = readCSV_by_column(filepath);
					if (input_csv != null) {
						// do stuff
					}
					break;
				case "linear":
					// do stuff
					break;
				case "hash":
					// do stuff
				}
			} else {
				// invalid user choice
				System.out.println("invalid choice!");
			}
		}
	}

	private static String[][] readCSV_by_column(String path) {
		File f = new File(path);
		if (!f.exists() || f.isDirectory()) {
			return null;
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line = br.readLine();
			ArrayList<String[]> table = new ArrayList<>();
			boolean header = true;
			ArrayList<String> name = new ArrayList<>();
			ArrayList<String> pwds = new ArrayList<>();
			ArrayList<String> gene = new ArrayList<>();

			while ((line = br.readLine()) != null) {
				if (header == false) {
					if (!line.isEmpty()) {
						String[] row = line.split(";");
						name.add(row[1]);
						pwds.add(row[2]);
						gene.add(row[3]);
					}
				}
				header = false;
			}

			String[][] result = { name.toArray(new String[name.size()]), name.toArray(new String[pwds.size()]),
					name.toArray(new String[gene.size()]) };
			return result;
		} catch (IOException ioe) {
			System.out.println("IOException thrown!");
			ioe.printStackTrace();
			return null;
		}
	}
}
