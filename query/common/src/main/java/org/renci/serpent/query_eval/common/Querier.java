package org.renci.serpent.query_eval.common;

import java.util.List;
import java.util.Properties;

import edu.ncsu.csc.coul.pathquery.QueryFilter;

public interface Querier {

	/**
	 * Initialize the querier
	 * @param datasetPath
	 * @param syntax
	 * @param p
	 */
	public void initialize(String datasetPath, String syntax, Properties p);
	
	/**
	 * Get a list of paths between src and destination
	 * @param src
	 * @param dst
	 * @param node query filter
	 * @param link query filter
	 * @param data set path
	 * @param data set syntax (Jena)
	 * @param variable number of string arguments specific to a querier
	 * @return
	 */
	public List<List<String>> getPaths(String src, String dst, QueryFilter nodeFilter, QueryFilter linkFilter);
	
	/**
	 * Perform actions on shutdown
	 */
	public void onShutdown();
}
