package org.renci.serpent.query_eval.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

public class Utils {
	protected static final Logger log = Logger.getLogger(Utils.class);
	
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
	
	/**
	 * Delete some files/directories recursively, if needed
	 * @param filesToDelete
	 */
	public static void deleteFiles(List<Path> filesToDelete) {
		for (Path p: filesToDelete) {
			try {
				Files.walkFileTree(p, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
							throws IOException
					{
						Files.delete(file);
						return FileVisitResult.CONTINUE;
					}
					@Override
					public FileVisitResult postVisitDirectory(Path dir, IOException e)
							throws IOException
					{
						if (e == null) {
							Files.delete(dir);
							return FileVisitResult.CONTINUE;
						} else {
							// directory iteration failed
							throw e;
						}
					}
				});
			} catch (Exception e) {
				log.error("Unable to delete " + p + " due to: " + e);
			}
		}
	}
	
	/**
	 * Create a temporary directory with specified prefix path
	 * @param prefix
	 * @return
	 */
	public static File createTempDirectory(String prefix) {
		File tmpDir = null;
		// create an ephemeral directory that goes away after JVM exits
		String tmpDirName = (prefix == null ? System.getProperty("java.io.tmpdir") : prefix) + 
				System.getProperty("file.separator") + "jenatdb" + UUID.randomUUID();
		tmpDir = new File(tmpDirName);
		if (!tmpDir.mkdir())
			return null;
		return tmpDir;
	} 
    
    /**
     * Recursive deletion
     * @param folder
     */
    public static void deleteDirectory(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
    
    public static void deleteDirectory(String p) {
        deleteDirectory(new File(p));
    }
}
