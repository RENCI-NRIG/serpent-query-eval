package org.renci.serpent.query_eval.drivers;

import java.io.FileInputStream;
import java.io.InputStream;
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

public class Main {
	private static Logger l = LogManager.getLogger("LOG"); 

	// map from names to classes of query engines
	enum EngineType {
		tarjan(org.renci.serpent.query_eval.tarjan.TarjanQuerier.class),
		gleen(org.renci.serpent.query_eval.gleen.GleenQuerier.class),
		//sparql(org.renci.serpent.query_eval.sparql.SparqlQuerier.class),
		neo4j(org.renci.serpent.query_eval.neo4j.Neo4jQuerier.class);

		private final Class<?> clazz;

		EngineType(Class<?> c) {
			clazz = c;
		}

		public Class<?> getEngineClass() {
			return clazz;
		}

		// match to a string name
		public static Class<?> getEngineClass(String n) {

			if (n == null)
				return null;
			for(EngineType t: EngineType.values()) {
				if (t.name().equalsIgnoreCase(n)) {
					return t.getEngineClass();
				}
			}
			return null;
		}
	}
	
	public enum PropName {
		CONSTRAINT("constraint"),
		TYPECONSTRAINT("type.constraint"),
		SRCS("src.list"),
		DSTS("dest.list"),
		TYPES("types.list"),
		LABELS("labels.list"),
		FACTS("facts.file"),
		SYNTAX("facts.file.syntax");
		
		public String pName; 
		private PropName(String nm) {
			pName = nm;
		}
	}

	public static void main(String[] argv) {
		Logger l = LogManager.getLogger("EVAL LOG");
		CommandLineParser  parser = new DefaultParser();
		Options options = new Options();

		options.addOption("c", true, "configuration properties file name");
		options.addOption("t", true, "type of query engine to use (tarjan, neo4j or gleen)");
		String propFile = null;
		Class engineClazz = null;

		try { 
			CommandLine line = parser.parse(options, argv, false);

			if (line.hasOption("h")) {
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("PathQueryDriver", options);
				System.exit(0);
			}

			if (line.hasOption("c")) {
				propFile = line.getOptionValue("c");
			}

			if (line.hasOption("t")) {
				engineClazz = EngineType.getEngineClass(line.getOptionValue("t"));
				if (engineClazz == null) {
					l.error("Unknown engine type " + line.getOptionValue("t") + ", exiting");
					System.exit(-1);
				}
			}
		} catch (ParseException pe) {
			l.error("Unable to parse command line: " + pe);
			System.exit(1);
		}

		Querier engine = null;
		try {
			engine = (Querier)engineClazz.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

		if (engine == null) {
			l.error("Unable to instantiate " + engineClazz.getName() + " engine, exiting");
			System.exit(-2);
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

		// install shutdown hook for the engine
		final Querier finalEngine = engine;
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				finalEngine.onShutdown();
			}
		});


	}
}
