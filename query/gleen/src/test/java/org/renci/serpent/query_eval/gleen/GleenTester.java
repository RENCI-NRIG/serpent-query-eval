package org.renci.serpent.query_eval.gleen;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.renci.serpent.query_eval.common.Querier.NodeRecord;

public class GleenTester { 

	public static void main(String[] argv) {
		GleenQuerier sq = new GleenQuerier();
		String[] dsts = {  "Node-395378", "Node-25899", "Node-27008", "Node-101", "Node-174", "Node-209", "Node-2914", "Node-3356", "Node-4826" };
		Logger.getRootLogger().setLevel(Level.INFO);
		try {
			Properties confProps = new Properties();
			confProps.setProperty("tmp.dir", "/Users/ibaldin/Desktop/SERPENT-WORK/TMP/");
			sq.initialize("/Users/ibaldin/Google Drive/Projects/SERPENT/CAIDA/20170201.as-rel2.txt.10node.n3", "N-Triples", null);
			for (String dst: dsts) {
				//String dst = "Node-101";
				Instant i1 = Instant.now();
				List<NodeRecord> qr = sq.getPaths("Node-395796", dst);
				Instant i2 = Instant.now();
				System.out.println("Query to " + dst + " took " + Duration.between(i1, i2).toMillis() + " ms");
				System.out.println("Query returned ");
				for(NodeRecord nr: qr) {
					System.out.println(nr.getIf1() + " --- " + nr.getNodename() + " --- " + nr.getIf2());
				}
			}
			sq.onShutdown();
			System.out.println("Exiting");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
