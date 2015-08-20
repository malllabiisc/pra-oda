package edu.cmu.ml.rtw.pra.experiments;

import iisc.serc.mall.Corpus;
import iisc.serc.mall.KB;
import iisc.serc.mall.Utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import com.google.common.annotations.VisibleForTesting;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.EmptyType;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.ml.rtw.pra.config.PraConfig;
import edu.cmu.ml.rtw.pra.features.FeatureGenerator;
import edu.cmu.ml.rtw.pra.features.FeatureMatrix;
import edu.cmu.ml.rtw.pra.features.PathType;
import edu.cmu.ml.rtw.pra.graphs.GraphCreator;
import edu.cmu.ml.rtw.pra.models.PraModel;
import edu.cmu.ml.rtw.users.matt.util.FileUtil;
import edu.cmu.ml.rtw.users.matt.util.Pair;

/**
 * Train and test PRA models in a couple of different configurations.
 * 
 * @author Matt Gardner (mg1@cs.cmu.edu)
 */
public class PraTrainAndTester {

	private static Logger logger = ChiLogger.getLogger("pra-driver");
	private final FileUtil fileUtil;

	public PraTrainAndTester() {
		this(new FileUtil());
	}

	@VisibleForTesting
	public PraTrainAndTester(FileUtil fileUtil) {
		this.fileUtil = fileUtil;
	}

	/**
	 * Given a list of (source, target) pairs (and a lot of other parameters),
	 * split them into training and testing and perform cross validation. In
	 * practice this just calls trainAndTest after splitting the data.
	 * 
	 * @param config
	 *            A {@link PraConfig} object that specifies where to find the
	 *            graph and training data, what options to use, and a lot of
	 *            other things.
	 */
	public void crossValidate(PraConfig config, KB kb, boolean isOnlineAug,
			String relation, long startTrainTime) throws IOException, SQLException {
		Pair<Dataset, Dataset> splitData = config.allData
				.splitData(config.percentTraining);
		Dataset trainingData = splitData.getLeft();
		Dataset testingData = splitData.getRight();
		config.outputter.outputSplitFiles(config.outputBase, trainingData,
				testingData);
		PraConfig.Builder builder = new PraConfig.Builder(config);
		builder.setAllData(null);
		builder.setPercentTraining(0);
		builder.setTrainingData(trainingData);
		builder.setTestingData(testingData);
		trainAndTest(builder.build(), kb, isOnlineAug, relation, startTrainTime);
	}

	/**
	 * Given a set of training examples and a set of testing examples, train and
	 * test a PRA model.
	 * 
	 * @param config
	 *            A {@link PraConfig} object that specifies where to find the
	 *            graph and training data, what options to use, and a lot of
	 *            other things.
	 */
	public void trainAndTest(PraConfig config, KB kb, Boolean isOnlineAug,
			String relation, long startTrainTime) throws IOException,
			SQLException {
		// PRA Training
		List<Pair<PathType, Double>> model = trainPraModel(config);
		long endTrainTime = System.currentTimeMillis();
		BufferedWriter trainTimeWriter = new BufferedWriter(new FileWriter(
				config.outputBase + "/trainingTimings.txt"));
		trainTimeWriter.write("Training took "
				+ (endTrainTime - startTrainTime) + " milliseconds\n");
		trainTimeWriter.close();
		// Profile Test time
		BufferedWriter testTimeWriter = new BufferedWriter(new FileWriter(
				config.outputBase + "/testTimings.txt"));
		long startTestTime = System.currentTimeMillis();
		// Bhushan, Perform Online Augmentation
		// Use the newly generated weights file
		if (isOnlineAug) {
			
			kb.setweightFilePath(config.outputBase + "//weights.tsv");
			kb.setOutputDir(config.outputBase);
			// Augment for all sources
			Set<String> sources = new HashSet<String>();
			logger.info("Augmenting during Test Time");
			for (int sourceID : config.testingData.getAllSources()) {
				sources.add(kb.getNodeDict().get(sourceID));
			}
			String[] parts = config.outputBase.split("/");
			String predictRelation = parts[parts.length - 1];
			logger.info("Processing Relation " + predictRelation);
			Corpus.runTestAugmentation(sources, kb, predictRelation);
			// 1. Update Dictionaries
			String graphDir = config.graph.replace("/edges.tsv", "");
			graphDir = graphDir.replace("graph_chi", "");
			config.nodeDict.setFromFile(graphDir + "node_dict.tsv");
			config.edgeDict.setFromFile(graphDir + "edge_dict.tsv");

			// 2. Reshard the graph, (first delete the shard files)
			Utils.deleteShards(config.graph.replace("edges.tsv", ""));
			GraphCreator gc = new GraphCreator("", false);
			gc.shardGraph(config.graph, 2);

			Map<Integer, List<Pair<Integer, Double>>> scores = testPraModel(
					config, model);
	
			// Reset to training time
			// copy training augmented file to relevant directories (overwrite)
			// Refresh the file for each relation
			FileUtils
					.deleteQuietly(new File(graphDir + "/graph_chi/edges.tsv"));
			FileUtils.copyFile(new File(graphDir + "/original/edges.tsv"),
					new File(graphDir + "/graph_chi/edges.tsv"));
		} else {
			Map<Integer, List<Pair<Integer, Double>>> scores = testPraModel(
					config, model);
		}
		// Profile Test time
		long endTestTime = System.currentTimeMillis();
		testTimeWriter.write("Testing took "
				+ (endTestTime - startTestTime) + " milliseconds\n");
		testTimeWriter.close();
	}

	/**
	 * Given a set of input data and some other parameters, this code runs three
	 * steps: selectPathFeatures, computeFeatureValues, and learnFeatureWeights,
	 * returning the results as a list of Pair<PathType, Double> weights.
	 * 
	 * @param config
	 *            A {@link PraConfig} object that specifies where to find the
	 *            graph and training data, what options to use, and a lot of
	 *            other things.
	 * 
	 * @return A learned model encoded as a list of
	 *         <code>Pair<PathType, Double></code> objects.
	 */
	public List<Pair<PathType, Double>> trainPraModel(PraConfig config) {
		FeatureGenerator generator = new FeatureGenerator(config);
		List<PathType> pathTypes = generator
				.selectPathFeatures(config.trainingData);
		String matrixOutput = null; // PraModel outputs split versions of the
									// training matrix
		FeatureMatrix featureMatrix = generator.computeFeatureValues(pathTypes,
				config.trainingData, matrixOutput);
		PraModel praModel = new PraModel(config);
		List<Double> weights = praModel.learnFeatureWeights(featureMatrix,
				config.trainingData, pathTypes);
		List<Pair<PathType, Double>> model = new ArrayList<Pair<PathType, Double>>();
		for (int i = 0; i < pathTypes.size(); i++) {
			model.add(new Pair<PathType, Double>(pathTypes.get(i), weights
					.get(i)));
		}
		return model;
	}

	/**
	 * Given a list of Pair<PathType, Double> weights and a set of test source
	 * nodes (with, optionally, a set of corresponding test target nodes that
	 * should be excluded from walks), return a set of (source, target, score)
	 * triples. We do not output any test results here, leaving that to the
	 * caller (but see, for instance, the <code>outputScores</code> method).
	 * 
	 * @param config
	 *            A {@link PraConfig} object that specifies where to find the
	 *            graph and training data, what options to use, and a lot of
	 *            other things.
	 * @param model
	 *            A list of PathTypes and associated weights that constitutes a
	 *            trained PRA model.
	 * 
	 * @return A map from source node to (target node, score) pairs, where the
	 *         score is computed from the features in the feature matrix and the
	 *         supplied weights. Note that there may be some sources from the
	 *         input list that have no corresponding scores. This means that
	 *         there were no paths of the input types from the source node to an
	 *         acceptable target.
	 */
	public Map<Integer, List<Pair<Integer, Double>>> testPraModel(
			PraConfig config, List<Pair<PathType, Double>> model) {
		List<PathType> pathTypes = new ArrayList<PathType>();
		List<Double> weights = new ArrayList<Double>();
		for (int i = 0; i < model.size(); i++) {
			if (model.get(i).getRight() == 0.0) {
				continue;
			}
			pathTypes.add(model.get(i).getLeft());
			weights.add(model.get(i).getRight());
		}
		String output = config.outputBase == null ? null : config.outputBase
				+ "test_matrix.tsv";
		FeatureGenerator generator = new FeatureGenerator(config);
		FeatureMatrix featureMatrix = generator.computeFeatureValues(pathTypes,
				config.testingData, output);
		PraModel praModel = new PraModel(config);
		Map<Integer, List<Pair<Integer, Double>>> scores = praModel
				.classifyInstances(featureMatrix, weights);
		// TODO(matt): analyze the scores here and output a result to the
		// command line? At least it
		// might be useful to have a "metrics.tsv" file, or something, that
		// computes some metrics over
		// these scores.
		config.outputter.outputScores(config.outputBase + "scores.tsv", scores,
				config);
		return scores;
	}
}
