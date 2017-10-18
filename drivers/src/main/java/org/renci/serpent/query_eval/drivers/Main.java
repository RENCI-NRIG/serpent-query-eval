package org.renci.serpent.query_eval.drivers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.renci.serpent.query_eval.common.Querier;
import org.renci.serpent.query_eval.common.Querier.NodeRecord;

public class Main {
	private static Logger l = LogManager.getLogger("LOG"); 

	// map from names to classes of query engines
	enum EngineType {
		tarjan(org.renci.serpent.query_eval.tarjan.TarjanQuerier.class),
		gleen(org.renci.serpent.query_eval.gleen.GleenQuerier.class),
		sparql(org.renci.serpent.query_eval.sparql.SparqlQuerier.class),
		neo4j(org.renci.serpent.query_eval.neo4j.Neo4jQuerier.class);

		private final Class<?> clazz;

		EngineType(Class<?> c) {
			clazz = c;
		}

		public Class<?> getEngineClass() {
			return clazz;
		}

		// match to a string name
		public static EngineType getEngineType(String n) {

			if (n == null)
				return null;
			for(EngineType t: EngineType.values()) {
				if (t.name().equalsIgnoreCase(n)) {
					return t;
				}
			}
			return null;
		}
	}
	
	public enum PropName {
		SRCS("src.list"),
		DSTS("dest.list"),
		FACTS("facts.file"),
		SYNTAX("facts.file.syntax");
		
		public String pName; 
		private PropName(String nm) {
			pName = nm;
		}
	}

	public static void main(String[] argv) {
		CommandLineParser  parser = new DefaultParser();
		Options options = new Options();

		options.addOption("c", true, "configuration properties file name");
		options.addOption("t", true, "type of query engine to use (tarjan, neo4j, sparql or gleen)");
		options.addOption("h", "helpful message");
		options.addOption("p", true, "file prefix specific to the query engine");
		options.addOption("f", true, "save results in CSV file here");
		String footer = "Engine data handling:\n" + 
		 "  * Gleen - executes locally (TDB) on local files\n" + 
		 "  * SPARQL - executes remotely (blazegraph) using local files\n" + 
		 "  * Neo4j - executes remotely (Neo4j) using either http:// or file:// URLs (in the filesystem of the server)\n" + 
		 "  * Tarjan - executes locally on local files\n" + 
		 "Fact files are specified relative to respective engine-specific prefixes - either local to the\n" + 
		 "execution filesystem (Gleen, Tarjan, SPARQL), or remote server file system (Neo4j) or URL (Neo4j).\n" + 
		 "The properties file only specifies the file name, and the appropriate prefix is provided on command line.";
		String propFile = null;
		EngineType engineType = null;
		String factsPrefix = null;
		String csvFileName = null;

		try { 
			CommandLine line = parser.parse(options, argv, false);

			if (line.hasOption("h")) {
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("PathQueryDriver", null, options, footer);
				System.exit(0);
			}

			if (line.hasOption("c")) {
				propFile = line.getOptionValue("c");
			}

			if (line.hasOption("t")) {
				engineType = EngineType.getEngineType(line.getOptionValue("t"));
				if (engineType == null) {
					l.error("Unknown engine type " + line.getOptionValue("t") + ", exiting");
					System.exit(-1);
				}
			}
			
			if (line.hasOption("p")) {
				factsPrefix = line.getOptionValue("p");
			}
			
			if (line.hasOption("f")) {
				csvFileName = line.getOptionValue("f");
			}
		} catch (ParseException pe) {
			l.error("Unable to parse command line: " + pe);
			System.exit(1);
		}
		
		if ((factsPrefix == null)  || (engineType == null) || (propFile == null) ){
			l.error("No query engine type, or properties file or prefix for facts file specified");
			System.exit(2);
		}

		// read in the properties file
		InputStream is = null;
		Properties props = new Properties();
		
		try {
			l.info("Loading configuration properties file " + propFile + " externally");
			is = new FileInputStream(propFile);

			// load properties
			props.load(is);
			is.close();
		} catch(Exception e) {
			l.error("Unable to open configuration properties file " + propFile + ", exiting");
			System.exit(1);
		}
		
		// parse out properties
		Map<PropName, String> configProps = new HashMap<>(); 
		for (PropName pn: PropName.values()) { 
			if (props.get(pn.pName) == null) {
				l.error("Property " + pn.pName + " is missing in the properties file, unable to proceed, exiting");
				System.exit(1);
			}
			configProps.put(pn, (String)props.get(pn.pName));
		}

		// create full facts file path
		String fullDataPath = factsPrefix + configProps.get(PropName.FACTS);
		l.info("Using " + fullDataPath + " facts file with syntax " + configProps.get(PropName.SYNTAX));
		
		Querier engine = null;
		try {
			l.info("Instantiating query engine " + engineType.name());
			engine = (Querier)engineType.getEngineClass().newInstance();
			
			// install shutdown hook for the engine
			final Querier finalEngine = engine;
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					finalEngine.onShutdown();
				}
			});
			
			/**
			 * Engines data handling
			 * Gleen - executes locally (TDB) on local files
			 * SPARQL - executes remotely (blazegraph) using local files
			 * Neo4j - executes remotely (Neo4j) using either http:// or file:/// URLs (in the filesystem of the server)
			 * Tarjan - executes locally on local files
			 */	
			
			// initialize engine
			engine.initialize(fullDataPath, configProps.get(PropName.SYNTAX), null);
		} catch (InstantiationException e) {
			e.printStackTrace();
			System.exit(255);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			System.exit(255);
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(255);
		}

		if (engine == null) {
			l.error("Unable to instantiate " + engineType.name() + " engine, exiting");
			System.exit(-2);
		}
		
		StringBuilder csvBuilder = new StringBuilder();
		
		try {
			String[] srcs = configProps.get(PropName.SRCS).split(",");
			String src = srcs[0];
			String[] dsts = configProps.get(PropName.DSTS).split(",");
			csvBuilder.append(configProps.get(PropName.FACTS) + ", SRC: " + src + "\n");
			csvBuilder.append("Destination, LinksOnPath, DurationMS\n");
			for (String dst: dsts) {
				csvBuilder.append(dst + ", ");
				l.info("Querying for path from " + src + " to " + dst);
				Instant i1 = Instant.now();
				List<NodeRecord> qr = engine.getPaths(src, dst);
				Instant i2 = Instant.now();
				// number of links is number of nodes +1
				csvBuilder.append(qr.size() + 1 + ", ");
				csvBuilder.append(Duration.between(i1, i2).toMillis() + "\n");
				l.info("Query took " + Duration.between(i1, i2).toMillis() + " ms");
				l.info("Query returned (" + qr.size() + ")");
				for(NodeRecord nr: qr) {
					l.info(nr.getIf1() + " --- " + nr.getNodename() + " --- " + nr.getIf2());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (csvFileName != null) {
			l.info("Saving csv file " + csvFileName);

			BufferedWriter bw = null;
			try {
				File propertyF = new File(csvFileName);
				bw = new BufferedWriter(new FileWriter(propertyF));
				bw.append(csvBuilder);
			} catch (Exception e) {
				l.error("Unable to save file " + csvFileName);
			} finally {
				if (bw != null) {
					try {
						bw.close();
					} catch (Exception e) {
						;
					}
				}
			}
		}
		l.info("Exiting");
	}
}
