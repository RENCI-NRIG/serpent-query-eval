package org.renci.serpent.query_eval.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

public class Tester {

	public static void main(String[] argv) {

		Driver driver = GraphDatabase.driver( "bolt://serpent.renci.org:7687", AuthTokens.basic( "neo4j", "$SERPENT$" ) );
		Session session = driver.session();

		Map<String, Object> parameters = new HashMap<>();
		parameters.put("name", "Arthur");
		parameters.put("title", "King");
		
		session.run("CREATE (a:Person {name: {name}, title: {title}})", parameters );

		StatementResult result = session.run( "MATCH (a:Person) WHERE a.name = {name} " +
				"RETURN a.name AS name, a.title AS title",
				parameters);
		
		while ( result.hasNext() )
		{
			Record record = result.next();
			System.out.println( record.get( "title" ).asString() + " " + record.get( "name" ).asString() );
		}

		session.close();
		driver.close();
	}
}