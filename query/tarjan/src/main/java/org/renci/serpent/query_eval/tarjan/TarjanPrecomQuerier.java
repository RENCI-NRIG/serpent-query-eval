package org.renci.serpent.query_eval.tarjan;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.renci.serpent.query_eval.common.Querier;
import org.renci.serpent.query_eval.common.Utils;

import cern.colt.matrix.impl.SparseObjectMatrix1D;
import edu.ncsu.csc.coul.pathquery.PSLoader;
import edu.ncsu.csc.coul.pathquery.QueryFilter;
import edu.ncsu.csc.coul.pathquery.QueryProcessor;
import edu.ncsu.csc.coul.pathquery.util.Global;

public class TarjanPrecomQuerier implements Querier {
	private static final String TMP_DIR = "tmp.dir";
	private static final String PFSOLVE_BDB_PREFIX = "pfsolve-bdb";
	private static final String NODE_TYPE_FILE = "node-type-file";
	private static final String NS = "http://code.renci.org/projects/serpent#";
	protected static final Logger log = Logger.getLogger(TarjanPrecomQuerier.class);
	protected final QueryProcessor qp = new QueryProcessor();
	protected List<Path> filesToDelete = new ArrayList<>();
	protected PSLoader psLoader = null;
	protected QueryFilter propertyFilter, nodeTypeFilter;
	SparseObjectMatrix1D resultMatrix;
	public static final String[] nodeTypes = { "http://schemas.ogf.org/nml/base/2013/02#Port", 
			"http://schemas.ogf.org/nml/base/2013/02#Node", 
			"http://schemas.ogf.org/nml/bgp/2017/03#CPLink",
			"http://schemas.ogf.org/nml/bgp/2017/03#PPLink"};
	public static final String[] propTypes = {"http://schemas.ogf.org/nml/base/2013/02#hasOutboundPort", 
			"http://schemas.ogf.org/nml/bgp/2017/03#isInboundPort", 
			"http://schemas.ogf.org/nml/bgp/2017/03#hasCPSink", 
			"http://schemas.ogf.org/nml/bgp/2017/03#isCPSource",
			"http://schemas.ogf.org/nml/bgp/2017/03#hasPPSink",
			"http://schemas.ogf.org/nml/bgp/2017/03#isPPSource"};
	
	Set<String> srcSet = new HashSet<>(), dstSet = new HashSet<>();
	
	public void initialize(String datasetPath, String syntax, Properties p) throws Exception {
		
		if ((p.getProperty("src") == null) && (p.getProperty("dst") == null)) 
			throw new Exception ("You must specify src and dst properties for this method");
		
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
		Instant in1 = Instant.now();
		qp.createPathSequence(datasetPath, syntax, "d");
		Instant in2 = Instant.now();
		log.info("Computing path sequences took " + Duration.between(in1, in2).toMillis() + "(ms)");
		
		log.info("Creating PS Loader");
		psLoader = new PSLoader(Global.getBdbPath().toString(), datasetPath, false);
		
		log.info("Computing src/dst solution");
		log.debug("   for " + p.getProperty("src") + "/" + p.getProperty("dst"));
		
		String[] srcAr = p.getProperty("src").split(",");
		String[] dstAr = p.getProperty("dst").split(",");
		
		String[] srcArFull = fullNameArray(srcAr);
		String[] dstArFull = fullNameArray(dstAr);
		
		in1 = Instant.now();
		resultMatrix = qp.solveStep("d", srcArFull, dstArFull, 
				psLoader.getPS(), psLoader.getContextPathSequence(), "F");
		in2 = Instant.now();
		log.info("Precompute SOLVE step took " + Duration.between(in1, in2).toMillis() + "(ms)");
		
		srcSet.addAll(Arrays.asList(srcAr));
		dstSet.addAll(Arrays.asList(dstAr));
	}

	private String[] fullNameArray(String[] ar) {
		String[] ret = new String[ar.length];
		for(int i = 0; i < ar.length; i++ ) {
			ret[i] = NS + ar[i];
		}
		return ret;
	}
	
	public List<NodeRecord> getPaths(String src, String dst) throws Exception {
		List<NodeRecord> ret = new ArrayList<>();
		
		if (!srcSet.contains(src) || !dstSet.contains(dst))
			throw new Exception("Src/dst pair has not been computed " + src + "/" + dst);
		
		qp.getOnlyPaths(NS + src, NS + dst, resultMatrix, propertyFilter, nodeTypeFilter, 
				psLoader.getContextPathSequence(), psLoader.getNodeTypeMap());
		
		List<LinkedList<String>> paths = qp.getPathNodes(NS + src, NS + dst);
		// we are only expecting one path on the list
		if (paths.size() != 1) 
			throw new Exception("QueryProcessor returned " + paths.size() + " paths instead of 1");
		
		List<String> path = paths.get(0);
		
		if (path == null) 
			throw new Exception("No path found");
		
		if (path.size() < 4)
			throw new Exception("Query processor returned a path of length " + path.size() + " instead of >= 4: " + path);
		
		// we only want the intermediate nodes, and QP returns start and end points as well, so skip 
		// first three elements and last three elements (port, node, link; link, node, port)
		path.remove(0);
		path.remove(0);
		path.remove(path.size()-1);
		path.remove(path.size()-1);
		
		Iterator<String> it = path.iterator();
		while (it.hasNext()) {
			String rdfNode = it.next();
			// just skip links
			if (rdfNode.contains("Link"))
				continue;
			
			String int1 = rdfNode;
			String nd = it.next();
			String int2 = it.next();
			NodeRecord nrec = new NodeRecord();
			nrec.setIf1(int1);
			nrec.setNodename(nd);
			nrec.setIf2(int2);
			ret.add(nrec);
		}
		
		return ret;
	}

	public void onShutdown() {
		Utils.deleteFiles(filesToDelete);
	}
}
