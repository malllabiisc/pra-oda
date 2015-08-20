/*@author Bhushan Kotnis
 * Performs the On Demand Augmentation using limited depth DFS
 */
package iisc.serc.mall;

import java.io.BufferedReader;
import java.io.BufferedWriter;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.sql.SQLException;
import java.util.*;

public class Corpus {

	// saves verb: SVOData

	private static StringBuilder augmentedGraph;
	private static StringBuilder augmentedEdgeDict;
	private static StringBuilder augmentedNodeDict;
	private static StringBuilder augmentedPaths;
	private static StringBuilder addedAliases;
	private static Set<String> statSet = new HashSet<String>();
	// private static StringBuilder addedInverses;

	private static Set<String> addedEdges = null;
	private static Map<String, String> addedRelations = new HashMap<String, String>();
	private static Map<String, Integer> addedNodes = new HashMap<String, Integer>();
	private static Set<String> newPaths = new HashSet<String>();
	private static Map<String, Integer> verbPathCounter = new HashMap<String, Integer>();
	private static Map<String, String> pathVerbPathMapper = new HashMap<String, String>();
	private static int maxNodesCounter;
	private static int maxRelationsCounter;

	public static KB initializeKB(String nodeDictFilePath,
			String edgeDictFilePath, String graphFilePath,
			String aliasesFilePath, String verbMappingsFilePath,
			String verbMappingsInversesFilePath, String splits,
			String inversesPath, String domainsPath, String outputDir, int K,
			int M) throws IOException, Exception {
		// Initialize KB
		KB nell = new KB(nodeDictFilePath, edgeDictFilePath, graphFilePath,
				aliasesFilePath, verbMappingsFilePath,
				verbMappingsInversesFilePath, splits, inversesPath,
				domainsPath, outputDir, K, M);
		return nell;
	}

	public static KB startTrainAugmentation(KB kb, String relation,
			Boolean isTrain) throws SQLException, IOException {

		generatePaths(null, kb, true, relation);
		commitFiles(kb, true);
		return kb;
	}

	/*
	 * Augmentation during test time
	 */
	public static void runTestAugmentation(Set<String> sources, KB kb,
			String predictRelation) throws IOException, SQLException {

		generatePaths(sources, kb, false, predictRelation);
		commitFiles(kb, false);

	}

	/*
	 * creates paths with verbs using DFS like recursion. KB source & verb --->
	 * x1, for each such x1, x1 & verb(s)---> x2 for each such x2, x2 & verb(s)
	 * ----> x3 .... for each such x3, x3& verb---> target
	 */
	private static void generatePaths(Set<String> sources, KB kb,
			boolean isTrain, String relation) throws IOException, SQLException {
		augmentedGraph = new StringBuilder();
		augmentedEdgeDict = new StringBuilder();
		augmentedNodeDict = new StringBuilder();
		augmentedPaths = new StringBuilder();
		addedAliases = new StringBuilder();
		// initialize added edges with present edges to avoid duplication
		if (addedEdges == null) {
			// cache this in KB
			addedEdges = readGraphFile(kb.getgraphPath());
		}
		addedNodes.clear();
		addedRelations.clear();
		newPaths.clear();
		verbPathCounter.clear();
		pathVerbPathMapper.clear();
		maxNodesCounter = kb.getNodeDict().size() + 1;
		maxRelationsCounter = kb.getEdgeDict().size() + 1;

		Map<String, Map<String, Set<String>>> trainingData = kb
				.getTrainingData();
		Set<String> filterWords = kb.getFilterWords();

		if (isTrain) {
			System.out.println("Processing Relation " + relation);
			for (int pathLength = 1; pathLength <= kb.getpathLength(); pathLength++) {
				augment(trainingData.get(relation), null, pathLength,
						filterWords, isTrain, kb);
			}
		} else {
			// test, do for a particular relation, set in the KB
			Set<String> paths = kb.readWeightsTranslatedFile(
					kb.getweightFilePath(), false);
			// package targets in sources
			Map<String, Set<String>> sourceTargetData = new HashMap<String, Set<String>>();
			// targets are the domain of the predicted relation
			Set<String> targets = kb.getDomains().get(relation);
			// all test sources for a fixed relation are matched with the
			// same target
			for (String source : sources) {
				sourceTargetData.put(source, targets);
			}
			// get the domain of the relation to be predicted.
			for (String p : paths) {
				// augment(sourceTargetData, p, kb);
				augment(sourceTargetData, p, 0, filterWords, isTrain, kb);
			}
		}
	}

	private static void augment(Map<String, Set<String>> sourceTargetData,
			String p, int pathLength, Set<String> filterWords, boolean isTrain,
			KB kb) throws IOException {
		ArrayList<ArrayList<String>> verbList = new ArrayList<ArrayList<String>>();

		if (!isTrain) {
			p = p.substring(0, p.length() - 1);
			System.out.println("Processing Path " + p);
			String[] relIDs = p.split("-");
			for (String relID : relIDs) {
				verbList.add(getVerbListForRelID(relID, kb));
			}
			pathLength = verbList.size();
		}
		for (String source : sourceTargetData.keySet()) {
			ArrayList<String> svoPath = new ArrayList<String>();

			DFSSVOGraph(0, new HashSet<Integer>(), -1, svoPath,
					sourceTargetData.get(source), verbList, source, pathLength,
					filterWords, isTrain, kb);

		}
	}

	/*
	 * DFS algorithm to discover paths from a given source to set of targets
	 * along a set of verbs
	 */
	private static void DFSSVOGraph(int level, Set<Integer> visited,
			int entityID, ArrayList<String> svoPath, Set<String> targets,
			ArrayList<ArrayList<String>> verbList, String kbSourceName,
			int pathLength, Set<String> filterWords, boolean isTrain, KB kb)
			throws IOException {

		if (level == pathLength + 1) {
			String obj = kb.getSVONodeDict().get(entityID);
			String matchedTarget = matchNounPhrase(obj, targets, kb);
			if (matchedTarget != null) {
				ArrayList<String> fullpath = new ArrayList<String>();
				fullpath.add(kbSourceName);
				fullpath.addAll(svoPath);
				String aliasPath = generateAliasPath(fullpath, matchedTarget,
						kb);
				if (!newPaths.contains(aliasPath)) {
					/*
					 * String filename = "PathOnTheFly.txt"; FileWriter fw = new
					 * FileWriter(filename, true); fw.write(aliasPath + "\n");
					 * fw.close();
					 */

					if (isTrain) {
						addToCounter(aliasPath);
					}
					newPaths.add(aliasPath);
				}
			}
			return;
		}

		if (level == 0) {
			Set<String> sourceAliases = kb.getAliases().get(kbSourceName);
			for (String alias : sourceAliases) {
				Integer matchedID = kb.getSVONodeDictInv().get(alias);
				if (matchedID != null) {
					visited.add(matchedID);
					svoPath.add(alias);
					level++;
					DFSSVOGraph(level, visited, matchedID, svoPath, targets,
							verbList, kbSourceName, pathLength, filterWords,
							isTrain, kb);
					svoPath.clear();
					visited.clear();
					level = 0;
				}
			}
		} else {
			Set<String> verbs = null;
			Map<String, ArrayList<Integer>> adjList = kb
					.getSVONeighborsForID(entityID);

			if (isTrain) {
				if (adjList != null)
					verbs = adjList.keySet();
			} else {
				verbs = new HashSet<String>(verbList.get(level - 1));
			}
			if (verbs != null) {
				for (String verb : verbs) {
					ArrayList<Integer> neighbors = null;
					if (adjList != null) {
						neighbors = adjList.getOrDefault(verb, null);
					}
					svoPath.add(verb);
					if (neighbors != null) {
						for (Integer neighborID : neighbors) {
							String entityName = kb.getSVONodeDict().get(
									neighborID);

							if (!visited.contains(neighborID)) {
								visited.add(neighborID);
								svoPath.add(entityName);
								level++;
								DFSSVOGraph(level, visited, neighborID,
										svoPath, targets, verbList,
										kbSourceName, pathLength, filterWords,
										isTrain, kb);
								svoPath.remove(svoPath.size() - 1);
								level--;
							}

						}
					}
					svoPath.remove(svoPath.size() - 1);
				}
			}
		}
	}

	/*
	 * Obtains verb list for RelID taking inverses into account.
	 */
	private static ArrayList<String> getVerbListForRelID(String relID, KB kb) {
		int relationID;
		ArrayList<String> verbs = new ArrayList<String>();
		// Handle Inverse relations

		if (relID.contains("_")) {
			relationID = Integer.parseInt(relID.substring(1));
			if (relationID > 520) {
				// Add _ and take care of it when getting SVO data
				verbs.add("_"
						+ kb.getEdgeDict().get(
								Integer.parseInt(relID.substring(1))));
			} else {
				// for < 520, _rels have verbs
				verbs = kb.getVerbMappings().get(relID);
			}
		} else {
			relationID = Integer.parseInt(relID);
			if (relationID > 520) {

				verbs.add(kb.getEdgeDict().get(relationID));
			} else {
				verbs = kb.getVerbMappings().get(relID);
			}
		}

		return verbs;
	}

	/*
	 * Matches noun phrase with given set of KB entities.
	 */
	private static String matchNounPhrase(String np, Set<String> entities, KB kb) {
		Map<String, Set<String>> invAliases = kb.getInverseAliases();
		Set<String> possibleMatches = invAliases.get(np);
		if (possibleMatches != null) {
			for (String possibleMatch : possibleMatches) {
				if (entities.contains(possibleMatch)) {
					return possibleMatch;
				}
			}
		}
		return null;
	}

	/*
	 * Adds the found path to the counter - counter:verbPath:{path1:count}
	 */
	private static void addToCounter(String path) {
		String[] elems = path.split(Constants.PATH_DELIM);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < elems.length; i++) {
			if (i % 2 == 1) {
				sb.append(elems[i]);
				sb.append(Constants.PATH_DELIM);
			}
		}
		String verbPath = sb.toString();

		int count = verbPathCounter.getOrDefault(verbPath, 0);
		verbPathCounter.put(verbPath, count + 1);

		if (!pathVerbPathMapper.containsKey(path)) {
			pathVerbPathMapper.put(path, verbPath);
		}
	}

	/*
	 * Adds @Alias@ verb for aliases at source and target
	 */
	private static String generateAliasPath(ArrayList<String> path,
			String target, KB kb) {

		StringBuilder sb = new StringBuilder();
		// Add @ALIAS@ at source
		sb.append(path.get(0) + Constants.PATH_DELIM);
		sb.append("_@ALIAS@" + Constants.PATH_DELIM);

		for (int i = 1; i < path.size(); i++) {
			sb.append(path.get(i) + Constants.PATH_DELIM);
		}

		// Add @ALIAS@ at target
		sb.append("@ALIAS@" + Constants.PATH_DELIM);
		sb.append(target);
		return sb.toString();
	}

	/*
	 * Processes the found path so that it can be inserted in node dict, edge
	 * dict and edges.tsv
	 */
	private static void processpath(String path, KB kb) throws IOException {

		writeStatistics(path, kb.getOutputDir());
		augmentedPaths.append(path + "\n");
		String[] elements = path.split(Constants.PATH_DELIM);
		Map<String, Integer> inverseNodeDict = kb.getInverseNodeDict();
		Map<String, Set<String>> inverseAliases = kb.getInverseAliases();
		Map<String, Integer> inverseEdgeDict = kb.getInverseEdgeDict();
		// Keeps track of node ID at each index
		Map<Integer, Integer> foundNodeID = new HashMap<Integer, Integer>();
		Map<Integer, String> foundRelationID = new HashMap<Integer, String>();
		Map<Integer, Boolean> invertFlags = new HashMap<Integer, Boolean>();
		// First add verbs and entities
		for (int index = 0; index < elements.length; index++) {
			// for Entity
			if (index % 2 == 0) {

				// check if already added
				if (!addedNodes.keySet().contains(elements[index])) {
					// check if entity in nodeDict
					if (inverseNodeDict.containsKey(elements[index])) {
						// if KB entity, (source entity)
						foundNodeID.put(index,
								inverseNodeDict.get(elements[index]));
						continue;
					}
					// Disambiguate entity by checking it in inverse aliases
					else if (inverseAliases.containsKey(elements[index])) {
						// check if they are purposely added ALIASEs
						if (index == 2 || index == elements.length - 3) {
							// add these aliases to KB
							foundNodeID.put(index, maxNodesCounter);
							augmentedNodeDict.append(Integer
									.toString(maxNodesCounter)
									+ "\t"
									+ elements[index] + "\n");
							addedNodes.put(elements[index], maxNodesCounter);
							addedAliases.append(elements[index] + "\t"
									+ elements[index] + "\n");
							maxNodesCounter++;
							continue;

						} else {
							// Disambiguate the bridging entity
							Set<String> kbNodes = inverseAliases
									.get(elements[index]);
							if (inverseNodeDict.get(kbNodes) != null) {
								String[] kbNode = (String[]) kbNodes.toArray();
								// get the first one
								int nodeID = inverseNodeDict.get(kbNode[0]);
								foundNodeID.put(index, nodeID);
								addedNodes.put(elements[index], nodeID);
								continue;
							} else {
								// This shouldnt be needed, but the inverse node
								// dict for some reason does not have ids for
								// some entities.
								// If no match in KB
								foundNodeID.put(index, maxNodesCounter);
								// create new entry
								augmentedNodeDict.append(Integer
										.toString(maxNodesCounter)
										+ "\t"
										+ elements[index] + "\n");
								addedNodes
										.put(elements[index], maxNodesCounter);
								addedAliases.append(elements[index] + "\t"
										+ elements[index] + "\n");
								maxNodesCounter++;
								continue;
							}
						}
					} else {
						// If no match in KB
						foundNodeID.put(index, maxNodesCounter);
						// create new entry
						augmentedNodeDict.append(Integer
								.toString(maxNodesCounter)
								+ "\t"
								+ elements[index] + "\n");
						addedNodes.put(elements[index], maxNodesCounter);
						addedAliases.append(elements[index] + "\t"
								+ elements[index] + "\n");
						maxNodesCounter++;
						continue;
					}

				}// if already present
				else {
					foundNodeID.put(index, addedNodes.get(elements[index]));
				}
			}
			// Verb
			if (index % 2 == 1) {
				// If _ then need to invert
				if (elements[index].startsWith("_")) {
					invertFlags.put(index, true);
					elements[index] = elements[index].replace("_", "");
				} else {
					invertFlags.put(index, false);
				}

				if (!addedRelations.keySet().contains(elements[index])) {
					// Edges already in KB, This could happen for NELL SVO
					if (inverseEdgeDict.containsKey(elements[index])) {
						String relID = inverseEdgeDict.get(elements[index])
								.toString();
						foundRelationID.put(index, relID);
						addedRelations.put(elements[index], relID);
					} else {
						// create new relation
						foundRelationID.put(index,
								Integer.toString(maxRelationsCounter));
						addedRelations.put(elements[index],
								Integer.toString(maxRelationsCounter));
						augmentedEdgeDict.append(Integer
								.toString(maxRelationsCounter)
								+ "\t"
								+ elements[index] + "\n");
						maxRelationsCounter++;
					}
				} else {
					foundRelationID.put(index,
							addedRelations.get(elements[index]));
				}
			}
		}

		// Another pass to add the edges in the graph
		for (int index = 0; index < elements.length; index++) {
			if (index % 2 == 1) {
				StringBuilder edge = new StringBuilder();

				if (invertFlags.get(index)) {
					// flip source target if invert is true
					edge.append(foundNodeID.get(index + 1) + "\t");
					edge.append(foundNodeID.get(index - 1) + "\t");
					edge.append(foundRelationID.get(index));
				} else {
					edge.append(foundNodeID.get(index - 1) + "\t");
					edge.append(foundNodeID.get(index + 1) + "\t");
					edge.append(foundRelationID.get(index));
				}
				// check if edge already present
				if (!addedEdges.contains(edge.toString())) {
					augmentedGraph.append(edge.toString() + "\n");
				}
			}
		}
	}

	/*
	 * This method writes number of bridging entities and verbs added by
	 * PRA-ODA.
	 */
	private static void writeStatistics(String path, String dir)
			throws IOException {
		BufferedWriter verbWriter = new BufferedWriter(new FileWriter(dir
				+ "verbStats.txt", true));
		BufferedWriter entityWriter = new BufferedWriter(new FileWriter(dir
				+ "entityStats.txt", true));
		String[] elems = path.split(Constants.PATH_DELIM);

		if (elems.length == 7) {
			verbEntityWriter(verbWriter, elems[3], true);

		} else if (elems.length == 9) {
			verbEntityWriter(entityWriter, elems[4], false);
			verbEntityWriter(verbWriter, elems[3], true);
			verbEntityWriter(verbWriter, elems[5], true);
		}

		verbWriter.close();
		entityWriter.close();
	}

	private static void verbEntityWriter(BufferedWriter bw, String word,
			boolean isVerb) throws IOException {
		if (!statSet.contains(word)) {
			bw.write(word + "\n");
			bw.flush();
			statSet.add(word);
		}
	}

	/*
	 * Get top K most frequent paths
	 */
	private static Set<String> filterMostFrequentPaths(int cutoff) {
		Set<String> filtered = new HashSet<String>();
		Map<Integer, Set<String>> inverted = new HashMap<Integer, Set<String>>();

		for (Map.Entry<String, Integer> entry : verbPathCounter.entrySet()) {
			if (inverted.containsKey(entry.getValue())) {
				inverted.get(entry.getValue()).add(entry.getKey());
			} else {
				inverted.put(entry.getValue(), new HashSet<String>());
				inverted.get(entry.getValue()).add(entry.getKey());
			}
		}
		List<Integer> frequencies = new ArrayList<Integer>(inverted.keySet());

		Collections.sort(frequencies, Collections.reverseOrder());
		if (frequencies.size() >= cutoff) {
			frequencies = frequencies.subList(0, cutoff);
		}

		for (Integer freq : frequencies) {

			if (freq > 1) {
				// Just get one verb
				for (String verb : inverted.get(freq)) {
					filtered.add(verb);
					// break;
				}
			}
		}
		return filtered;
	}

	/*
	 * Filter the paths and write the files to disk
	 */
	private static void commitFiles(KB kb, boolean isTrain) throws IOException {

		boolean isProcess = false;
		Set<String> filtered = null;
		if (isTrain)
			filtered = filterMostFrequentPaths(kb.getK());
		for (String path : newPaths) {
			if (isTrain) {

				String verbPath = pathVerbPathMapper.get(path);
				if (filtered.contains(verbPath)) {
					processpath(path, kb);
					isProcess = true;
				}
			} else {
				processpath(path, kb);
				isProcess = true;
			}
		}

		if (isProcess) {
			kb.setNodeDict(Utils.appendStringBuilderToDict(kb.getNodeDict(),
					augmentedNodeDict));
			kb.setInverseNodeDict(Utils.appendStringBuilderToInverseDict(
					kb.getInverseNodeDict(), augmentedNodeDict));
			// update edgeDict
			kb.setEdgeDict(Utils.appendStringBuilderToDict(kb.getEdgeDict(),
					augmentedEdgeDict));
			kb.setInverseEdgeDict(Utils.appendStringBuilderToInverseDict(
					kb.getInverseEdgeDict(), augmentedEdgeDict));

			// update aliases

			kb.setAliases(Utils.appendStringBuilderToMap(kb.getAliases(),
					addedAliases, false));
			kb.setInverseAliases(Utils.appendStringBuilderToInvAliases(
					kb.getInverseAliases(), addedAliases));

			writeToFile(kb.getnodeDictPath(), augmentedNodeDict);
			writeToFile(kb.getedgeDictPath(), augmentedEdgeDict);
			writeToFile(kb.getgraphPath(), augmentedGraph);
			writeToFile(kb.getOutputDir() + Constants.PATHS_FILE,
					augmentedPaths.append("-----------------------" + "\n"));
		}
	}

	/*
	 * Reads the edges.tsv file and puts it in a set to prevent duplicates
	 */
	private static Set<String> readGraphFile(String fileName)
			throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String line = "";
		Set<String> presentEdges = new HashSet<String>();
		while ((line = br.readLine()) != null) {
			presentEdges.add(line);
		}
		br.close();
		return presentEdges;
	}

	private static void writeToFile(String filename, StringBuilder content)
			throws IOException {
		// Node Dicts
		FileWriter wr;
		BufferedWriter bw;
		wr = new FileWriter(filename, true);
		bw = new BufferedWriter(wr);
		bw.write(content.toString());
		bw.close();
	}
}
