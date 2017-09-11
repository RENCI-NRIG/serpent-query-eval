package org.renci.serpent.query_eval.sparql;

import java.util.List;
import java.util.Properties;

import org.renci.serpent.query_eval.common.Querier;

import edu.ncsu.csc.coul.pathquery.QueryFilter;

/**
 * Hello world!
 *
 */
public class SparqlQuerier implements Querier  {

	public void initialize(String datasetPath, String syntax, Properties p) {
		// TODO Auto-generated method stub
		
	}

	public List<List<String>> getPaths(String src, String dst, QueryFilter nodeFilter, QueryFilter linkFilter) {
		// TODO Auto-generated method stub
		return null;
	}

	public void onShutdown() {
		// TODO Auto-generated method stub
		
	}
}
