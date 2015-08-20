/*
 * @author Bhushan Kotnis
 */
package iisc.serc.mall;

public class Constants {
	public static final String INDEX_DIR = "/scratch/bhushan/project/svoindex/";
	public static final String SVO_EDGE_DICT = "/scratch/bhushan/project/nell_hazy_data/"+"verbDict.tsv";
	public static final int SOURCE = 0;
	public static final int TARGET = 1;
	public static final int HARD_MATCH = 0;
	public static final int JACCARD = 1;
	public static final int SUBSTRING = 2;
	public static final int ONEWORD = 3;
	// Threshold must be between 0 and 1
	public static final double JACCARD_THRESH = 0.8;
	public static final String STOP_WORDS = "/scratch/bhushan/project/nltk_data/corpora/stopwords/english";
	public static final String PATHS_FILE = "/addedPaths.txt";
	public static final String PATH_DELIM = "->";

	
	public static final char[] alphabet = "abcdefghijklmnopqrstuvwxyz"
			.toCharArray();
	public static final String USER = "neo4j";
	public static final String PASS = "nell";
	
}
