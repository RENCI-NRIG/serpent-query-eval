package org.renci.serpent.query_eval.tarjan;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.renci.serpent.query_eval.common.Querier;
import org.renci.serpent.query_eval.common.Utils;

import edu.ncsu.csc.coul.pathquery.PSLoader;
import edu.ncsu.csc.coul.pathquery.QueryFilter;
import edu.ncsu.csc.coul.pathquery.QueryProcessor;
import edu.ncsu.csc.coul.pathquery.util.Global;

public class TarjanQuerier implements Querier {
	private static final String TMP_DIR = "tmp.dir";
	private static final String PFSOLVE_BDB_PREFIX = "pfsolve-bdb";
	private static final String NODE_TYPE_FILE = "node-type-file";
	private static final String NS = "http://code.renci.org/projects/serpent#";
	protected static final Logger log = Logger.getLogger(TarjanQuerier.class);
	protected final QueryProcessor qp = new QueryProcessor();
	protected List<Path> filesToDelete = new ArrayList<>();
	protected PSLoader psLoader = null;
	protected QueryFilter propertyFilter, nodeTypeFilter;
	public static final String[] nodeTypes = { "http://schemas.ogf.org/nml/base/2013/02#Port", 
			"http://schemas.ogf.org/nml/base/2013/02#Node", 
			"http://schemas.ogf.org/nml/bgp/2017/03#CPLink"};
	public static final String[] propTypes = {"http://schemas.ogf.org/nml/base/2013/02#hasOutboundPort", 
			"http://schemas.ogf.org/nml/bgp/2017/03#isInboundPort", 
			"http://schemas.ogf.org/nml/bgp/2017/03#hasCPSink", 
			"http://schemas.ogf.org/nml/bgp/2017/03#isCPSource"};
	
	public void initialize(String datasetPath, String syntax, Properties p) throws Exception {
		// create filters for nodes and properties
		if ((p != null) && (p.containsKey(TMP_DIR)))
			Global.setBdbPath(Files.createTempDirectory(Paths.get(p.getProperty(TMP_DIR)), PFSOLVE_BDB_PREFIX));
		else 
			Global.setBdbPath(Files.createTempDirectory(PFSOLVE_BDB_PREFIX));
		
		Global.setNodeTypePath(Files.createTempFile(NODE_TYPE_FILE, "ser"));
		filesToDelete.add(Global.getBdbPath());
		filesToDelete.add(Global.getNodeTypePath());
		
		log.info("Generating query filters");
	
		nodeTypeFilter = new QueryFilter(QueryFilter.Constraint.ONLY, nodeTypes);
		propertyFilter = new QueryFilter(QueryFilter.Constraint.ONLY, propTypes);
		
		log.info("Generating initial path sequence");
		qp.createPathSequence(datasetPath, syntax, "d");
		
		log.info("Creating PS Loader");
		psLoader = new PSLoader(Global.getBdbPath().toString(), datasetPath, false);
	}

	public List<NodeRecord> getPaths(String src, String dst) throws Exception {
		String[] srcs = { NS + src };
		String[] dsts = { NS + dst };
		qp.getAllPathsInMemory("d", 
				srcs, dsts, 
				propertyFilter, nodeTypeFilter,
				psLoader.getPS(), psLoader.getContextPathSequence(),"F");
		return null;
	}

	public void onShutdown() {
		Utils.deleteFiles(filesToDelete);
		
	}
}
