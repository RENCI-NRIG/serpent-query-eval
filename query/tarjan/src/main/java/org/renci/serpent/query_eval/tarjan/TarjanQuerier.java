package org.renci.serpent.query_eval.tarjan;

import java.util.List;
import java.util.Properties;

import org.renci.serpent.query_eval.common.Querier;

import edu.ncsu.csc.coul.pathquery.QueryFilter;

public class TarjanQuerier implements Querier {

	public void onShutdown() {
		// TODO Auto-generated method stub
		
	}

	public void initialize(String datasetPath, String syntax, Properties p) throws Exception {
		QueryFilter q = null;
	}

	public List<NodeRecord> getPaths(String src, String dst) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
