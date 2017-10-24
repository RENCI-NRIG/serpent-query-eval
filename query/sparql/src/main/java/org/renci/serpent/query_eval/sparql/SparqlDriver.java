package org.renci.serpent.query_eval.sparql;

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
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.renci.serpent.query_eval.common.Querier;
import org.renci.serpent.query_eval.common.Querier.NodeRecord;

public class SparqlDriver {
	private static Logger l = LogManager.getLogger("LOG"); 
	
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
		//Logger.getRootLogger().setLevel(Level.WARN);

		options.addOption("c", true, "configuration properties file name");
		options.addOption("h", "helpful message");
		options.addOption("p", true, "file prefix specific to the query engine");
		options.addOption("f", true, "save results in CSV file here");
		String footer = "This is for SPARQL only.";
		String propFile = null;
		String factsPrefix = null;
		String csvFileName = null;
		SparqlQuerier sq = new SparqlQuerier();

		Logger.getRootLogger().setLevel(Level.INFO);
		
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
		
		if ((factsPrefix == null)  || (propFile == null) ){
			l.error("No properties file or prefix for facts file specified");
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

		try {
			
			// install shutdown hook for the engine
			final Querier finalEngine = sq;
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
			//sq.initialize(fullDataPath, configProps.get(PropName.SYNTAX), null);
			sq.initialize("/Users/ibaldin/Desktop/SERPENT-WORK/EVAL/20170201.as-rel2.txt.100node.n3", "N-Triples", null);
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
				List<NodeRecord> qr = sq.getPaths(src, dst);
				Instant i2 = Instant.now();
				// number of links is number of nodes +1
				csvBuilder.append((qr != null ? qr.size() : 0) + 1 + ", ");
				csvBuilder.append(Duration.between(i1, i2).toMillis() + "\n");
				l.info("Query took " + Duration.between(i1, i2).toMillis() + " ms");
				l.info("Query returned (" + (qr != null ? qr.size() : 0) + ")");
				if (qr != null)
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
		System.exit(0);
	}
}
