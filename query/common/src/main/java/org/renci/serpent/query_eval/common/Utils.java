package org.renci.serpent.query_eval.common;

public class Utils {

	/**
	 * There are different definitions of RDFFormat, so making class path explicit
	 * @param syntax
	 * @return
	 */
	public static org.openrdf.rio.RDFFormat formatFromString(String syntax) {
		if (syntax == null)
			return org.openrdf.rio.RDFFormat.NTRIPLES;
		if ("RDF/XML".equals(syntax))
			return org.openrdf.rio.RDFFormat.RDFXML;
		if ("TURTLE".equals(syntax) || "TTL".equals(syntax))
			return org.openrdf.rio.RDFFormat.TURTLE;
		return org.openrdf.rio.RDFFormat.NTRIPLES;
	}
}
