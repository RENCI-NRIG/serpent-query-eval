package org.renci.serpent.query_eval.neo4j;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;
import org.renci.serpent.query_eval.common.Querier;

public class Neo4jQuerier implements Querier {

	private static final String SERPENT_NS = "http://code.renci.org/projects/serpent#";
	private static final String NEO4J_CREDENTIALS_PROPERTIES = "/org/renci/serpent/query_eval/neo4j/neo4j-credentials.properties";
	protected Driver driver = null;
	protected Session session = null;
	protected static final Logger log = Logger.getLogger(Neo4jQuerier.class);
	private static final String ns0 = "ns0", ns1 = "ns1";

	/**
	 * Load the database with new dataset
	 */
	public void initialize(String datasetPath, String syntax, Properties p) throws Exception {
		Properties neo4jProps = p;
		
		if (neo4jProps == null) {
			neo4jProps = new Properties();
		
			InputStream in = getClass().getResourceAsStream(NEO4J_CREDENTIALS_PROPERTIES);
			if (in == null) {
				throw new Exception("Unable to find credential properties");
			}
			neo4jProps.load(in);
			in.close();
		}
		
		driver = GraphDatabase.driver(neo4jProps.getProperty("bolt.url"), 
				AuthTokens.basic( neo4jProps.getProperty("username"), 
						neo4jProps.getProperty("password") ) );
		session = driver.session();
		
		// load the dataset
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("url", datasetPath);
		parameters.put("format", syntax);
		
		// use external procedure to load RDF dataset
		StatementResult result = session.run("call semantics.importRDF({url}, {format}, {shortenUrls: true, typesToLabels: true, commitSize: 9000})", parameters);

		if (result.hasNext()) {
			Record record = result.next();
			if ((record != null) && ("OK".equals(record.get("terminationStatus").asString()))) {
				log.info("Dataset " + datasetPath + " loaded");
			}
			else
				throw new Exception("Unable to properly load dataset " + datasetPath);
		} else
			throw new Exception("Unable to properly load dataset " + datasetPath);
		
		// create an index
		session.run("CREATE INDEX ON :" + ns0 + "_Node(uri)");
	}

	public void onShutdown() {
		session.run("MATCH (N) DETACH DELETE N");
		session.close();
		driver.close();
	}

	public List<NodeRecord> getPaths(String src, String dst) throws Exception {
		String query = "match (a:" + ns0 + "_Node {uri: {srcurl}}), " + 
			"(z:" + ns0 + "_Node {uri: {dsturl}}), " + 
			"p=(a) -[*1..]-> (z) " +  
			//"where none(x in nodes(p) where 'ns0_SwitchingService' in labels(x)) " +
			"where ALL(x in rels(p) where ((type(x) = '" + ns0 + "_hasOutboundPort') OR (type(x) = '" + ns1 + "_isInboundPort') " + 
			"OR (type(x) =  '" + ns1 + "_hasCPSink') OR (type(x) = '" + ns1 + "_isCPSource') OR (type(x) = '" + ns1 + "_isPPSource') OR (type(x) = '" + ns1 + "_hasPPSink'))) " + 
			"AND all(x IN nodes(p) WHERE ('" + ns0 + "_Node' IN labels(x)) OR ('" + ns0 + "_Port' IN labels(x)) OR ('" + ns1 + "_CPLink' IN labels(x)) OR ('" + ns1 + "_PPLink' IN labels(x))) " +
			"return p, filter(x in nodes(p) where NOT (('" + ns1 + "_CPLink' IN labels(x)) OR ('" + ns1 + "_PPLink' IN labels(x)))) AS l";

		Map<String, Object> queryParams = new HashMap<>();
		queryParams.put("srcurl", SERPENT_NS + src);
		queryParams.put("dsturl", SERPENT_NS + dst);
		
		StatementResult result = session.run(query, queryParams);
		
		List<NodeRecord> ret = new ArrayList<>();
		while(result.hasNext()) {
			Record r = result.next();
			//Path p = r.get("p").asPath();
			//Iterator<Node> iter = p.nodes().iterator();
			//while(iter.hasNext()) {
			//	Node n = iter.next();
			//	log.info(n.get("uri"));
			//}
			//log.info(r);
			List<Object> ln = r.get("l").asList();
			List<Object> nl = new ArrayList<>(ln);
			
			// remove first two (node and port) and last two (node and port) for now
			nl.remove(0);
			nl.remove(0);
			nl.remove(nl.size() - 1);
			nl.remove(nl.size() - 1);
			Iterator<Object> it = nl.iterator();
			while (it.hasNext()) {
				Node int1 = (Node)it.next();
				Node nd = (Node)it.next();
				Node int2 = (Node)it.next();
				NodeRecord nrec = new NodeRecord();
				nrec.setIf1(int1.get("uri").asString());
				nrec.setNodename(nd.get("uri").asString());
				nrec.setIf2(int2.get("uri").asString());
				ret.add(nrec);
			}
		}
		
		return ret;
	}

}
