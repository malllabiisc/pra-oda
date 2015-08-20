/*
 * @author Bhushan Kotnis
 */
package iisc.serc.mall;

import java.io.File;
import java.io.IOException;


import org.apache.commons.io.FileUtils;

public class OnlineAugment {
	/*
	 * Augments the KB and returns the new KB
	 */
	public static KB init(String kbDirectory, String graphDirectory,
			String splitsDirectory, String outputDir,
			int k, int pathLength) throws IOException, Exception {

		String nodeDictFilePath = graphDirectory + "//node_dict.tsv";
		String edgeDictFilePath = graphDirectory + "//edge_dict.tsv";
		String originalNodeDict = graphDirectory + "//original/node_dict.tsv";
		String originalEdgeDict = graphDirectory + "//original/edge_dict.tsv";
		String originalGraphFile = graphDirectory + "//original/edges.tsv";
		String graphFilePath = graphDirectory + "//graph_chi//edges.tsv";
		String aliasesFilePath = kbDirectory + "//aliases.tsv";
		String verbMappingsFilePath = kbDirectory + "//RelMappings.txt";
		String verbMappingsInversesFilePath = kbDirectory
				+ "//RelInverseMappings.txt";
		String inverses = kbDirectory + "//inversesID.tsv";
		String domains = kbDirectory + "//relations";
		
		String splits = splitsDirectory;

		// Clear graph_chi and copy nodeDict, edgeDict, aliases, edges
		
		FileUtils.cleanDirectory(new File(graphDirectory + "//graph_chi"));
		FileUtils
				.copyFile(new File(originalGraphFile), new File(graphFilePath));
		FileUtils.deleteQuietly(new File(nodeDictFilePath));
		FileUtils.copyFile(new File(originalNodeDict), new File(
				nodeDictFilePath));
		FileUtils.deleteQuietly(new File(edgeDictFilePath));
		FileUtils.copyFile(new File(originalEdgeDict), new File(
				edgeDictFilePath));
	
		KB kb = Corpus.initializeKB(nodeDictFilePath,
				edgeDictFilePath, graphFilePath, aliasesFilePath,
				verbMappingsFilePath, verbMappingsInversesFilePath,
				 splits, inverses, domains, outputDir, k, pathLength);
		
		return kb;

	}
}
