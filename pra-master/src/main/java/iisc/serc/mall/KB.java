/*
 * @author Bhushan Kotnis
 * Reads all the required stuff from the KB directories
 */
package iisc.serc.mall;



import static com.mongodb.client.model.Filters.eq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.Map.Entry;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.bson.Document;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.google.common.base.Splitter;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import createmapdb.Custom_Values.Neighbors;

public class KB {

	// members do not need constructor. Initialize them in init functions
	private Map<Integer, String> nodeDict;
	private Map<String, Integer> inverseNodeDict;
	private Map<Integer, String> edgeDict;
	private Map<String, Integer> inverseEdgeDict;
	// Load SVO Graph to RAM
	private Map<Integer, String> svoNodeDict;
	private Map<String, Integer> svoNodeDictInv;
	private Map<Integer, String> svoEdgeDict;
	private Map<String, Integer> svoEdgeDictInv;
	private Map<Integer, Map<String, Set<Integer>>> svoAdjList;

	//private Map<Integer, Neighbors> svoDBAdjList;
	// Source Targets are stored as IDs (strings)
	private Map<String, Map<String, Set<String>>> trainingData;
	// Entities stored as names
	private Map<String, Set<String>> aliases;
	// Stored as ID (string)
	private Map<String, Set<String>> inverseAliases;
	// Stored as ID (string)
	private Map<String, String> inverses;
	// Relations stored as ID (string)
	private Map<String, ArrayList<String>> verbMappings;
	// Used only during test time
	private Map<String, Set<String>> domains;

	private Map<String, ArrayList<String>> verbCache;

	private Map<String, Set<String>> topPaths;
	// File Paths
	private MongoClient mongoClient;
	MongoCollection<Document> collAdj;
	MongoCollection<Document> collNodeDict;
	DB nodeDictDB;
	private String nodeDictPath;
	private String edgeDictPath;
	private String graphPath;
	private String aliasesPath;
	private String verbMappingsPath;
	private String verbMappingsInversesPath;
	private String splitsDir;
	private String inversesPath;
	private String domainsPath;
	// path of single weight file
	private String weightFilePath;
	// k of top k paths
	private String outputDir;
	private int K;
	private int pathLength;

	// private Graph svoGraph;

	public KB() {
		// default constructor for faster debugging
	}

	public KB(String nodeDictFilePath, String edgeDictFilePath,
			String graphFilePath, String aliasesFilePath,
			String verbMappingsFilePath, String verbMappingsInversesFilePath,
			String splits, String inversesFilePath, String domainsPath,
			String outputDir, int K, int pathLength) throws IOException,
			NotBoundException {

		this.initPaths(nodeDictFilePath, edgeDictFilePath, graphFilePath,
				aliasesFilePath, verbMappingsFilePath,
				verbMappingsInversesFilePath, splits, inversesFilePath,
				domainsPath, outputDir);
		this.K = K;
		this.pathLength = pathLength;
		this.initializeKB();

	}

	public int getK() {
		return this.K;
	}

	public int getpathLength() {
		return this.pathLength;
	}

	public String getnodeDictPath() {
		return this.nodeDictPath;
	}

	public String getedgeDictPath() {
		return this.edgeDictPath;
	}

	public String getgraphPath() {
		return this.graphPath;
	}

	public String getaliasesPath() {
		return this.aliasesPath;
	}

	public String getinversesPath() {
		return this.inversesPath;
	}

	public String getweightFilePath() {
		return this.weightFilePath;
	}

	

	public void setweightFilePath(String fileName) {
		this.weightFilePath = fileName;
	}

	public Map<Integer, String> getNodeDict() {
		return this.nodeDict;
	}

	public void setNodeDict(Map<Integer, String> nodeDict) {
		this.nodeDict = nodeDict;
	}

	public Map<String, Integer> getInverseNodeDict() {
		return this.inverseNodeDict;
	}

	public void setInverseNodeDict(Map<String, Integer> inversenodeDict) {
		this.inverseNodeDict = inversenodeDict;
	}

	public Map<Integer, String> getEdgeDict() {
		return this.edgeDict;
	}

	public void setEdgeDict(Map<Integer, String> edgeDict) {
		this.edgeDict = edgeDict;
	}

	public Map<String, Integer> getInverseEdgeDict() {
		return this.inverseEdgeDict;
	}

	public void setInverseEdgeDict(Map<String, Integer> inverseedgeDict) {
		this.inverseEdgeDict = inverseedgeDict;
	}
/*
	private void initSVODBAdjList() {
		String base = "/scratch/bhushan/project/nell_hazy_data/";
		File nodeDictFile = new File(base + "adjList.mdb");
		DB adjListDB = DBMaker.newFileDB(nodeDictFile).mmapFileEnable()
				.cacheSize(10000000).make();
		this.svoDBAdjList = adjListDB.getHashMap("svoAdjList");
	}
*/
	/*
	 * @Deprecated public String getSVONodeName(int nodeID) throws
	 * RemoteException { return this.svoGraph.getNodeName(nodeID); }
	 * 
	 * @Deprecated public Integer getSVONodeID(String name) throws
	 * RemoteException { return this.svoGraph.getNodeID(name); }
	 * 
	 * @Deprecated public Map<String, Set<Integer>> getSVOVerbs(Integer nodeID)
	 * throws RemoteException { return this.svoGraph.getVerbs(nodeID); }
	 */

	public void setSVONodeDict(Map<Integer, String> nodeDict) {
		this.svoNodeDict = nodeDict;
		/*
		 * this.svoNodeDictInv = new HashMap<String, Integer>(nodeDict.size());
		 * for (Entry<Integer, String> entry : this.svoNodeDict.entrySet()) {
		 * this.svoNodeDictInv.put(entry.getValue(), entry.getKey()); }
		 */
	}

	public Map<Integer, String> getSVOEdgeDict() {
		return this.svoEdgeDict;
	}

	public Map<Integer, Map<String, Set<Integer>>> getSVOAdjList() {
		return this.svoAdjList;
	}

	public void setSVOAdjList(Map<Integer, Map<String, Set<Integer>>> adjList) {
		this.svoAdjList = adjList;
	}

	public Map<String, Integer> getSVONodeDictInv() {
		return this.svoNodeDictInv;
	}

	public Map<Integer, String> getSVONodeDict() {
		return this.svoNodeDict;
	}

	public Map<String, Integer> getSVOEdgeDictInv() {
		return this.svoEdgeDictInv;
	}

	public Map<String, Set<String>> getAliases() {
		return this.aliases;
	}

	public void setAliases(Map<String, Set<String>> aliases) {
		this.aliases = aliases;
	}

	public Map<String, Set<String>> getInverseAliases() {
		return this.inverseAliases;
	}

	public void setInverseAliases(Map<String, Set<String>> inverseAliases) {
		this.inverseAliases = inverseAliases;
	}

	public Map<String, ArrayList<String>> getVerbMappings() {
		return this.verbMappings;
	}

	public Map<String, Map<String, Set<String>>> getTrainingData() {
		return this.trainingData;
	}

	public Map<String, String> getInverses() {
		return this.inverses;

	}

	public Map<String, Set<String>> getDomains() {
		return this.domains;
	}

	public Map<String, Set<String>> getTopKPaths() {
		return this.topPaths;
	}

	public void setVerbCache(Map<String, ArrayList<String>> verbCache) {
		this.verbCache = verbCache;
	}

	public Map<String, ArrayList<String>> getVerbCache() {
		if (this.verbCache == null) {
			return new HashMap<String, ArrayList<String>>();
		} else {
			return this.verbCache;
		}
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

	public String getOutputDir() {
		return this.outputDir;
	}

	@Deprecated
	public String getSVONodeName(int nodeID) {
		Document myDoc = this.collNodeDict.find(eq("_id", nodeID)).first();
		if (myDoc == null) {
			return null;
		} else
			return myDoc.getString("name").toString();
	}

	@Deprecated
	public Integer getSVONodeID(String name) {
		Document myDoc = this.collNodeDict.find(eq("name", name)).first();
		if (myDoc == null)
			return null;
		else
			return (int) myDoc.getInteger("_id");
	}

	/*
	 * Reads neighbors from MongoDB
	 */
	public Map<String, ArrayList<Integer>> getSVONeighborsForID(int subjID) {
		// return this.svoAdjList.get(subjID);
		/*
		 * Neighbors neighbors = this.svoDBAdjList.get(subjID);
		 * if(neighbors!=null) return neighbors.getNeighbors(); else return
		 * null;
		 */
		Document myDoc = this.collAdj.find(eq("_id", subjID)).first();
		Map<String, ArrayList<Integer>> neighbors = null;
		if (myDoc != null) {
			ArrayList<Integer> verbIDs = (ArrayList<Integer>) myDoc
					.get("verbs");
			if (verbIDs != null) {
				neighbors = new HashMap<String, ArrayList<Integer>>();
				for (Integer verbID : verbIDs) {
					neighbors.put(this.svoEdgeDict.get(verbID),
							(ArrayList<Integer>) myDoc.get(verbID.toString()));
				}
			}
		}
		return neighbors;

	}

	public void closeDB() {
		this.mongoClient.close();
		this.nodeDictDB.close();
	}

	private void initPaths(String nodeDictFilePath, String edgeDictFilePath,
			String graphFilePath, String aliasesFilePath,
			String verbMappingsFilePath, String verbMappingsInversesFilePath,
			String splits, String inversesFilePath, String domainsPath,
			String outputDir) {
		this.nodeDictPath = nodeDictFilePath;
		this.edgeDictPath = edgeDictFilePath;
		this.graphPath = graphFilePath;
		this.aliasesPath = aliasesFilePath;
		this.verbMappingsPath = verbMappingsFilePath;
		this.verbMappingsInversesPath = verbMappingsInversesFilePath;
		this.splitsDir = splits;
		this.inversesPath = inversesFilePath;
		this.domainsPath = domainsPath;
		this.outputDir = outputDir;
	}

	private void initializeKB() throws IOException {
		// Read in the following order
		initMongoDB();
		readAllDicts();
		readAliases();
		readInverses();
		readDomains();
		readVerbMappings();
		readVerbMappingsInverses();
		readTrainingData();
		// initSVODBAdjList();
		// readTopKPathsWeightFile();
		// readSVOAdjList();

	}

	public void refreshAliases() throws IOException {
		readAliases();
	}

	private void initMongoDB() {
		this.mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("SVO");
		this.collAdj = db.getCollection("ADJList");
	}

	/*
	 * Initializes the node and edge dictionaries
	 */
	private void readAllDicts() throws IOException {
		readDict("node");
		readDict("edge");
		readDict("SVOedge");
		readSVONodeDict();



	}

	private void readSVONodeDict() throws IOException {
		
		String base = "/scratch/bhushan/project/nell_hazy_data/";
		File nodeDictFile = new File(base + "nodeDict.mdb");
		nodeDictDB = DBMaker.newFileDB(nodeDictFile).mmapFileEnable()
				.cacheSize(10000000).make();
		this.svoNodeDict = nodeDictDB.getHashMap("svoNodeDict");
		this.svoNodeDictInv = nodeDictDB.getHashMap("svoNodeDictInv");

	}

	private void readDict(String type) throws IOException,
			IllegalArgumentException {
		Map<Integer, String> dict;
		Map<String, Integer> inverseDict;
		String fileName = "";
		String seperator = "\t";
		if (type.equals("node")) {
			dict = new HashMap<Integer, String>();
			inverseDict = new HashMap<String, Integer>();
			fileName = this.nodeDictPath;

		} else if (type.equals("edge")) {
			dict = new HashMap<Integer, String>();
			inverseDict = new HashMap<String, Integer>();
			fileName = this.edgeDictPath;
			this.edgeDict = new HashMap<Integer, String>();
			this.inverseEdgeDict = new HashMap<String, Integer>();

		} else if (type.equals("SVOedge")) {
			dict = new HashMap<Integer, String>();
			inverseDict = new HashMap<String, Integer>();
			fileName = Constants.SVO_EDGE_DICT;
			this.svoEdgeDict = new HashMap<Integer, String>();
			this.svoEdgeDictInv = new HashMap<String, Integer>();
			seperator = "\t";
		} else {
			throw new IllegalArgumentException();
		}

		List<String> lines = Files.readAllLines(Paths.get(fileName),
				Charset.defaultCharset());
		// byte[] encoded = Files.readAllBytes(Paths.get(fileName));
		// String result = new String(encoded, Charset.defaultCharset());
		// Iterable<String> lines = Splitter.on('\n').omitEmptyStrings()
		// .split(result);
		for (String line : lines) {
			String[] words = line.split(seperator);
			// No need to check if key is already present. Unique ids.
			dict.put(Integer.parseInt(words[0]), words[1]);
			inverseDict.put(words[1], Integer.parseInt(words[0]));
		}
		if (type.equals("node")) {
			this.nodeDict = dict;
			this.inverseNodeDict = inverseDict;
			// this.nodeDict.putAll(dict);
			// this.inverseNodeDict.putAll(inverseDict);

		} else if (type.equals("edge")) {
			this.edgeDict.putAll(dict);
			this.inverseEdgeDict.putAll(inverseDict);
		} else if (type.equals("SVOedge")) {
			this.svoEdgeDict.putAll(dict);
			this.svoEdgeDictInv.putAll(inverseDict);
		} else if (type.equals("SVOnode")) {
			// Do not copy the huge dict
			this.svoNodeDict = dict;
			this.svoNodeDictInv = inverseDict;
		}
		// No finally block needed, program should terminate on an exception.

	}

	/*
	 * Reads the aliases file.
	 */
	private void readAliases() throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(this.aliasesPath));
		String result = new String(encoded, Charset.defaultCharset());
		Iterable<String> lines = Splitter.on('\n').split(result);
		// List<String> lines = Files.readAllLines(Paths.get(this.aliasesPath),
		// Charset.defaultCharset());

		// int size = (int) (4051686.00 / 0.75) + 1;
		this.aliases = new HashMap<String, Set<String>>();
		this.inverseAliases = new HashMap<String, Set<String>>();
		for (String line : lines) {
			String[] words = line.split("\t");

			Set<String> temp = new HashSet<String>();
			String key = words[0].trim().toLowerCase();
			for (int i = 1; i < words.length; i++) {
				String alias = words[i].trim().toLowerCase();
				temp.add(alias);
				if (this.inverseAliases.containsKey(alias)) {
					this.inverseAliases.get(alias).add(key);
				} else {
					this.inverseAliases.put(alias, new HashSet<String>());
					this.inverseAliases.get(alias).add(key);
				}
			}
			this.aliases.put(key, temp);
		}
	}

	/*
	 * Reads the inverses file.
	 */
	private void readInverses() throws IOException {

		List<String> lines = Files.readAllLines(Paths.get(this.inversesPath),
				Charset.defaultCharset());

		// int size = 1000;
		this.inverses = new HashMap<String, String>();
		for (String line : lines) {
			String[] words = line.split("\t");
			// Get only the first alias
			this.inverses.put(words[0], words[1]);
		}
	}

	/*
	 * Read domain instances of each KB relation
	 */
	private void readDomains() throws IOException {
		File dir = new File(this.domainsPath);
		this.domains = new HashMap<String, Set<String>>();
		String[] files = dir.list();
		BufferedReader br = null;
		for (String file : files) {
			if (new File(this.domainsPath + "//" + file).isFile()) {
				br = new BufferedReader(new InputStreamReader(
						new FileInputStream(this.domainsPath + "//" + file)));
				Set<String> concepts = new HashSet<String>();
				String line = "";
				while ((line = br.readLine()) != null) {
					String[] words = line.split("\t");
					concepts.add(words[1]);
				}
				this.domains.put(file.replace("_", ":"), concepts);
				br.close();
			}
		}
	}

	/*
	 * Reads the verb-mappings file
	 */
	private void readVerbMappings() throws IOException {

		this.verbMappings = new HashMap<String, ArrayList<String>>(500);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(this.verbMappingsPath)));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] words = line.split("\t");
			String[] verbs = words[1].split(",");
			ArrayList<String> filteredVerbs = new ArrayList<String>();
			for (int i = 0; i < verbs.length; i++) {
				if (i < this.pathLength) {
					filteredVerbs.add(verbs[i]);
				} else {
					break;
				}
			}

			this.verbMappings.put(words[0].trim(), filteredVerbs);
		}
		br.close();

	}

	private void readVerbMappingsInverses() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(this.verbMappingsInversesPath)));

		String line = "";
		while ((line = br.readLine()) != null) {
			String[] words = line.split("\t");
			String[] verbs = words[1].split(",");

			if (verbs[0].equalsIgnoreCase("no_verb_found_in_SVO")) {
				// If no verb found get the verb list of its KB inverse
				this.verbMappings.put(words[0], this.verbMappings
						.get(this.inverses.get(words[0].replace("_", ""))));
			} else {
				ArrayList<String> filteredVerbs = new ArrayList<String>();
				for (int i = 0; i < verbs.length; i++) {
					if (i < this.pathLength) {
						filteredVerbs.add(verbs[i]);
					} else {
						break;
					}
				}
				this.verbMappings.put(words[0], filteredVerbs);
			}
		}
		br.close();
	}

	
	/*
	 * Returns the top K paths as a set of strings "-relID-_relID-". Adds
	 * predicted relation to path
	 */
	public Set<String> readWeightsTranslatedFile(String fileName,
			boolean isTrain) throws IOException {
		Set<String> paths = new HashSet<String>();

		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(fileName)));
		String line = "";

		while ((line = br.readLine()) != null) {
			// if anything goes wrong skip the path
			boolean flag = false;
			StringBuilder path = new StringBuilder();
			String pathNames[] = line.split("\t");
			if (!line.contains("generalizations")) {
				// Remove @ALIAS@- relation
				String pathName = pathNames[0].replace("_@ALIAS@-", "");
				pathName = pathName.replace("@ALIAS@-", "");
				if (!isTrain) {
					if (line.contains("concept:")) {
						// for test, skip relations that contain KB relations
						continue;
					}
				}
				String[] relations = pathName.split("-");
				// Diminishing returns on higher order paths, therefore
				// restrict.
				if (relations.length > 4
						|| Double.parseDouble(pathNames[1]) <= 0.1) {
					continue;
				}
				for (int i = 0; i < relations.length; i++) {
					// relation starts with concept:hence to eliminate
					// blanks
					if (!relations[i].equals("")) {
						String relation = null;
						String relID = null;
						if (relations[i].startsWith("_")) {
							// Handle _, by first removing it to get the id
							relation = relations[i].substring(1);
							if (!this.inverseEdgeDict.containsKey(relation)) {
								flag = true;
								System.out.println(pathName);
								System.out.println("The Offending Relation :"
										+ relation);
							} else {
								relID = "_"
										+ this.inverseEdgeDict.get(relation)
												.toString();
							}
							// Use verbs from _, instead of inverting
							// relID = this.inverses.get(relID);
						} else {

							if (!this.inverseEdgeDict.containsKey(relations[i])) {
								flag = true;
								System.out.println(pathName);
								System.out.println("The Offending Relation :"
										+ relation);
							} else {

								relID = this.inverseEdgeDict.get(relations[i])
										.toString();
							}
						}

						if (!flag) {
							path.append(relID.trim() + "-");
						}
					}
				}
				if (!flag) {
					paths.add(path.toString());
				}

				if (paths.size() >= this.K) {
					break;
				}

			}
		}
		br.close();
		return paths;
	}

	/*
	 * Reads the training data.
	 */
	public void readTrainingData() throws IOException {
		trainingData = new HashMap<String, Map<String, Set<String>>>();

		String[] names = new File(this.splitsDir).list();
		// TODO : Change for Windows/Linux
		for (String name : names) {
			if (new File(this.splitsDir + "//" + name).isDirectory()) {
				FileInputStream fPositive = new FileInputStream(this.splitsDir
						+ "//" + name + "//" + "training.tsv");
				Map<String, Set<String>> training = this.readFromTrainingFiles(
						fPositive, new HashMap<String, Set<String>>());
				trainingData.put(name, training);
			}
		}

	}

	private Map<String, Set<String>> readFromTrainingFiles(FileInputStream f,
			Map<String, Set<String>> sourceTarget) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(f));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] words = line.split("\t");
			if (sourceTarget.containsKey(words[0])) {
				sourceTarget.get(words[0]).add(words[1]);
			} else {
				Set<String> targets = new HashSet<String>();
				targets.add(words[1]);
				sourceTarget.put(words[0], targets);
			}
		}
		br.close();
		f.close();
		return sourceTarget;
	}

	/*
	 * Collect aliases of KB entities that are of interest.
	 */
	public Set<String> getFilterWords() throws IOException {
		File dir = new File(this.domainsPath);
		Set<String> words = new HashSet<String>();
		String[] files = dir.list();
		BufferedReader br = null;
		for (String file : files) {
			if (new File(this.domainsPath + "//" + file).isFile()) {
				br = new BufferedReader(new InputStreamReader(
						new FileInputStream(this.domainsPath + "//" + file)));

				String line = "";
				while ((line = br.readLine()) != null) {
					String[] parts = line.split("\t");
					words.addAll(this.aliases.get(parts[0]));
					words.addAll(this.aliases.get(parts[1]));
				}
			}
		}
		br.close();
		return words;
	}

}
