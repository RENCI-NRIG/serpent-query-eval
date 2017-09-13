package org.renci.serpent.query_eval.common;

import java.util.List;
import java.util.Properties;

import edu.ncsu.csc.coul.pathquery.QueryFilter;

public interface Querier {

	public class NodeRecord {
		public String getIf1() {
			return if1;
		}

		public void setIf1(String if1) {
			this.if1 = if1;
		}

		public String getIf2() {
			return if2;
		}

		public void setIf2(String if2) {
			this.if2 = if2;
		}

		public String getNodename() {
			return nodename;
		}

		public void setNodename(String nodename) {
			this.nodename = nodename;
		}

		String if1, if2, nodename;
		
	}
	/**
	 * Initialize the querier
	 * @param datasetPath
	 * @param syntax
	 * @param p
	 */
	public void initialize(String datasetPath, String syntax, Properties p) throws Exception;
	
	/**
	 * Get a list of paths between src and destination
	 * @param src
	 * @param dst
	 * @return
	 */
	public List<NodeRecord> getPaths(String src, String dst) throws Exception;
	
	/**
	 * Perform actions on shutdown
	 */
	public void onShutdown();
}
