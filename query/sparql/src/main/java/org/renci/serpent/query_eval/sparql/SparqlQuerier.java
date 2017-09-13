package org.renci.serpent.query_eval.sparql;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.rio.RDFFormat;
import org.renci.serpent.query_eval.common.Querier;
import org.renci.serpent.query_eval.common.Utils;

import com.bigdata.rdf.sail.webapp.SD;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.JettyResponseListener;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * Heavily borrowed from Sesame API examples for Blazegraph
 * https://wiki.blazegraph.com/wiki/index.php/Sesame_API_remote_mode
 *
 */
public class SparqlQuerier implements Querier  {
	protected RemoteRepositoryManager repoMan;
	// DO NOT ADD TRAILING SLASH!!!
	protected String serviceUrl = "http://localhost:9999/blazegraph";
	
	protected static final Logger log = Logger.getLogger(SparqlQuerier.class);
	protected String namespace = null;

	public void initialize(String datasetPath, String syntax, Properties p) throws Exception {
		Logger.getRootLogger().setLevel(Level.WARN);
		
		repoMan = new RemoteRepositoryManager(serviceUrl, false);

		JettyResponseListener response = getStatus();
		log.info(response.getResponseBody());

		Path dsp = Paths.get(datasetPath);

		// create a new namespace if not exists
		namespace = dsp.getFileName().toString().replaceAll("\\.", "-");

		final Properties properties = new Properties();
		properties.setProperty("com.bigdata.rdf.sail.namespace", namespace);

		if (!namespaceExists(namespace)) {
			log.info(String.format("Create namespace %s...", namespace));
			repoMan.createRepository(namespace, properties);
			log.info(String.format("Create namespace %s done", namespace));
		} else {
			log.info(String.format("Namespace %s already exists", namespace));
		}

		//get properties for namespace
		//response = getNamespaceProperties(namespace);
		//log.info(String.format("Property list for namespace %s", namespace));
		//log.info(response.getResponseBody());

		loadDataFromFile(datasetPath, Utils.formatFromString(syntax));
	}


	/**
	 * Expects both to be strings of type Node-<number> indicating the AS
	 */
	public List<NodeRecord> getPaths(String src, String dst) throws Exception {
		String query = "prefix nml: <http://schemas.ogf.org/nml/base/2013/02#>\n" + 
				"prefix bgp: <http://schemas.ogf.org/nml/bgp/2017/03#>\n" + 
				"prefix : <http://code.renci.org/projects/serpent#>\n" + 		
				"prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\n" + 
				"SELECT DISTINCT ?inPort ?x ?outPort\n" +
					"WHERE {\n" +
					":" + src + " ( nml:hasOutboundPort / ( bgp:isCPSource | bgp:isPPSource ) / ( bgp:hasCPSink | bgp:hasPPSink ) / bgp:isInboundPort )* / nml:hasOutboundPort / ( bgp:isCPSource | bgp:isPPSource ) / ( bgp:hasCPSink | bgp:hasPPSink )  ?inPort .\n" + 
					"?inPort bgp:isInboundPort ?x .\n" + 
					"?x rdf:type nml:Node .\n" +
					"{ \n" + 
					"SELECT DISTINCT ?x ?outPort\n" + 
					"WHERE {\n" +
						"?x nml:hasOutboundPort ?outPort .\n" + 
						"?outPort ( bgp:isCPSource | bgp:isPPSource ) / ( bgp:hasCPSink | bgp:hasPPSink ) / bgp:isInboundPort / ( nml:hasOutboundPort / ( bgp:isCPSource | bgp:isPPSource ) / ( bgp:hasCPSink | bgp:hasPPSink ) / bgp:isInboundPort )* :" + dst + " .\n" + 
						"} } }";
		//result processing
		TupleQueryResult result = null;
		List<NodeRecord> ret = new ArrayList<>();
		
		try {
			// execute query
			result = repoMan.getRepositoryForNamespace(namespace)
					.prepareTupleQuery(query).evaluate();
	
			while (result.hasNext()) {
				final BindingSet bs = result.next();
				NodeRecord nr = new NodeRecord();
				nr.setIf1(bs.getBinding("inPort").getValue().stringValue());
				nr.setNodename(bs.getBinding("x").getValue().stringValue());
				nr.setIf2(bs.getBinding("outPort").getValue().stringValue());
				ret.add(nr);
			}
		} finally {
			if (result != null)
				result.close();
		}
		return ret;
	}
	

	public void onShutdown() {
		try {
			repoMan.deleteRepository(namespace);
			repoMan.close();
		} catch (Exception e) {
			
		}
	}
	
	private JettyResponseListener getStatus()
			throws Exception {

		final ConnectOptions opts = new ConnectOptions(serviceUrl + "/status");
		opts.method = "GET";
		return repoMan.doConnect(opts);

	}

	/*
	 * Check namespace already exists.
	 */
	private boolean namespaceExists(final String namespace) throws Exception {
		
		final GraphQueryResult res = repoMan.getRepositoryDescriptions();
		try {
			while (res.hasNext()) {
				final Statement stmt = res.next();
				if (stmt.getPredicate()
						.toString()
						.equals(SD.KB_NAMESPACE.stringValue())) {
					if (namespace.equals(stmt.getObject().stringValue())) {
						return true;
					}
				}
			}
		} finally {
			res.close();
		}
		return false;
	}

	/*
	 * Get namespace properties.
	 */
	private JettyResponseListener getNamespaceProperties(final String namespace) throws Exception {

		final ConnectOptions opts = new ConnectOptions(serviceUrl + "/namespace/"
				+ namespace + "/properties");
		opts.method = "GET";
		return repoMan.doConnect(opts);

	}

	/*
	 * Load data into a namespace.
	 */
	private void loadDataFromResource(final String namespace, final String resource) throws Exception {
		final InputStream is = SesameRemote.class
				.getResourceAsStream(resource);
		if (is == null) {
			throw new IOException("Could not locate resource: " + resource);
		}
		try {
			repoMan.getRepositoryForNamespace(namespace).add(
					new RemoteRepository.AddOp(is, RDFFormat.N3));
		} finally {
			is.close();
		}
	}
	
	/*
	 * Load data from file
	 */
	private void loadDataFromFile(final String filename, final RDFFormat format) throws Exception {
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(filename);
			repoMan.getRepositoryForNamespace(namespace).add(
					new RemoteRepository.AddOp(fis, format));
		} finally {
			if (fis != null)
				fis.close();
		}
	}
}
