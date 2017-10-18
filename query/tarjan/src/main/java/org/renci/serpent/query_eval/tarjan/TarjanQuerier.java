package org.renci.serpent.query_eval.tarjan;

import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.renci.serpent.query_eval.common.Querier;

import edu.ncsu.csc.coul.pathquery.QueryFilter;
import edu.ncsu.csc.coul.pathquery.QueryProcessor;

public class TarjanQuerier implements Querier {

	protected static final Logger log = Logger.getLogger(TarjanQuerier.class);
	protected final QueryProcessor qp = new QueryProcessor();
	
	public void initialize(String datasetPath, String syntax, Properties p) throws Exception {
		// create filters for nodes and properties
		
		
		QueryFilter q = null;
	}

	public List<NodeRecord> getPaths(String src, String dst) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	public void onShutdown() {
		
		
	}

	
}
