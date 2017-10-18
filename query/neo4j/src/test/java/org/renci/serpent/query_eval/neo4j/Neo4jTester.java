package org.renci.serpent.query_eval.neo4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.renci.serpent.query_eval.common.Querier.NodeRecord;

public class Neo4jTester {

	public static void main(String argv[]) {
		Neo4jQuerier nq = new Neo4jQuerier();
		String[] dsts = {  "Node-395378", "Node-25899", "Node-27008", "Node-101", "Node-174", "Node-209", "Node-2914", "Node-3356", "Node-4826" };
		//String[] dsts = {  "Node-27008"};
		Logger.getRootLogger().setLevel(Level.INFO);
		try {
			// note that file:// URL schema works assuming files are locatable on the filesystem of the Neo4j server
			Properties neo4j = new Properties();
			neo4j.put("bolt.url", "bolt://hostname:7687");
			neo4j.put("username", "user");
			neo4j.put("password", "pass");
			// pass in properties or null for defaults
			nq.initialize("http://geni-images.renci.org/images/ibaldin/SERPENT/20170201.as-rel2.txt.100node.ttl", "Turtle", null);
			for (String dst: dsts) {
				Instant i1 = Instant.now();
				List<NodeRecord> qr = nq.getPaths("Node-395796", dst);
				Instant i2 = Instant.now();
				System.out.println("Query to " + dst + " took " + Duration.between(i1, i2).toMillis() + " ms");
				System.out.println("Query returned ");
				for(NodeRecord nr: qr) {
					System.out.println(nr.getIf1() + " --- " + nr.getNodename() + " --- " + nr.getIf2());
				}
			}
			nq.onShutdown();
			System.out.println("Exiting");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
