package org.renci.serpent.query_eval.gleen;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.renci.serpent.query_eval.common.Querier;
import org.renci.serpent.query_eval.common.Utils;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.ResultBinding;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileManager;

public class GleenQuerier implements Querier {
	private static final String TMP_DIR = "tmp.dir";
	protected OntModel model = null;
	protected Dataset ds = null;
	protected File dir = null;
	protected static final Logger log = Logger.getLogger(GleenQuerier.class);
	
	public void onShutdown() {
		TDB.sync(model);
		model.close();
		ds.end();
		Utils.deleteDirectory(dir);
	}

	public void initialize(String datasetPath, String syntax, Properties p) throws Exception {
		InputStream in = FileManager.get().openNoMap(datasetPath);

		OntModelSpec spec = OntModelSpec.OWL_MEM;

		if ((p != null) && (p.containsKey(TMP_DIR)))
			dir = Utils.createTempDirectory(p.getProperty(TMP_DIR));
		else
			dir = Utils.createTempDirectory(null);

		if (dir == null)
			throw new Exception("Unable to create temporary model folder.");
		log.info("Created temporary directory " + dir.getAbsolutePath());

		ds = TDBFactory.createDataset(dir.getAbsolutePath());
		Model fileModel = ds.getDefaultModel();
		model = ModelFactory.createOntologyModel(spec, fileModel);

        model.read(in, null, syntax);
        TDB.sync(model);
        
        in.close();
	}

	public List<NodeRecord> getPaths(String src, String dst) throws Exception {
		String queryString1 = "PREFIX gleen:<java:edu.washington.sig.gleen.> " + 
				"prefix nml: <http://schemas.ogf.org/nml/base/2013/02#>\n" + 
				"prefix bgp: <http://schemas.ogf.org/nml/bgp/2017/03#>\n" + 
				"prefix : <http://code.renci.org/projects/serpent#>\n" + 		
				"prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\n" +
				"SELECT DISTINCT ?x ?inPort\n" +
				"WHERE {\n" +
					":" + src + " gleen:OnPath(\"( [nml:hasOutboundPort] / ( [bgp:isCPSource] | [bgp:isPPSource] ) / ( [bgp:hasCPSink] | [bgp:hasPPSink] ) / [bgp:isInboundPort] )* / [nml:hasOutboundPort] / ( [bgp:isCPSource] | [bgp:isPPSource] ) / ( [bgp:hasCPSink] | [bgp:hasPPSink] )\"  ?inPort) .\n" +
					"?inPort bgp:isInboundPort ?x .\n" + 
					"?x rdf:type nml:Node .\n" + 
				"}";
		String queryString2 = "PREFIX gleen:<java:edu.washington.sig.gleen.> " + 
				"prefix nml: <http://schemas.ogf.org/nml/base/2013/02#>\n" + 
				"prefix bgp: <http://schemas.ogf.org/nml/bgp/2017/03#>\n" + 
				"prefix : <http://code.renci.org/projects/serpent#>\n" + 		
				"prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\n" +
				"SELECT DISTINCT ?x ?outPort\n" +
				"WHERE {\n" +
					"?outPort gleen:OnPath(\"( [bgp:isCPSource] | [bgp:isPPSource] ) / ( [bgp:hasCPSink] | [bgp:hasPPSink] ) / [bgp:isInboundPort] / ( [nml:hasOutboundPort] / ( [bgp:isCPSource] | [bgp:isPPSource] ) / ( [bgp:hasCPSink] | [bgp:hasPPSink] ) / [bgp:isInboundPort] )*\" :" + dst + " ) .\n" +
					"?x nml:hasOutboundPort ?outPort .\n" +
					"?x rdf:type nml:Node .\n" + 
				"}";

		Query query1 = QueryFactory.create(queryString1);
		Query query2 = QueryFactory.create(queryString2);

		// Execute the query and obtain results
		QueryExecution qe1 = QueryExecutionFactory.create(query1, model);
		ResultSet results1 = qe1.execSelect();
		QueryExecution qe2 = QueryExecutionFactory.create(query2, model);
		ResultSet results2 = qe2.execSelect();
		
		// do a join manually
		HashMap<Resource, Resource> inPortMap = new HashMap<>();
		
		List<NodeRecord> ret = new ArrayList<>();
		HashSet<Resource> resSet1 = new HashSet<>();
		while(results1.hasNext()) {
			ResultBinding result = (ResultBinding)results1.next();
			if (result != null) {
				Resource r = (Resource)result.get("x");
				resSet1.add(r);
				inPortMap.put(r, (Resource)result.get("inPort"));
			}
		}
		while(results2.hasNext()) {
			ResultBinding result = (ResultBinding)results2.next();
			if (result != null)
				if (resSet1.contains((Resource)result.get("x"))) {
					Resource xNode = (Resource)result.get("x");
					NodeRecord nr = new NodeRecord();
					nr.setNodename(xNode.getLocalName());
					Resource outPortResource = (Resource)result.get("outPort");
					nr.setIf2(outPortResource.getLocalName());
					nr.setIf1(inPortMap.get(xNode).getLocalName());
					ret.add(nr);
				}
		}
		return ret;
	}

}
