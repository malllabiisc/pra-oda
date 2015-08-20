/*
 * @author Bhushan Kotnis
 */
package iisc.serc.mall;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import edu.cmu.ml.rtw.users.matt.util.Dictionary;

public class Utils {

	private static Set<String> stopWords;

	/*
	 * Returns a string which does not contain stop words. Converts words to
	 * lower case
	 */
	public static String filterStopWords(String line) throws IOException {
		if (stopWords == null) {
			initStopWords();
		}

		if (!line.equals("")) {
			line = line.toLowerCase();
			StringBuilder filteredWords = new StringBuilder();
			String[] words = line.split(" ");
			for (int i = 0; i < words.length; i++) {
				String word = words[i];

				if (stopWords.contains(word)) {
					continue;
				} else {
					filteredWords.append(word + " ");
				}
			}
			return filteredWords.toString().trim();
		}
		return "";
	}

	/*
	 * Dont forget to change the stop words directory.
	 */
	private static void initStopWords() throws IOException {
		FileReader f = new FileReader(Constants.STOP_WORDS);
		BufferedReader br = new BufferedReader(f);
		String line = "";
		stopWords = new HashSet<String>();
		while ((line = br.readLine()) != null) {
			stopWords.add(line.trim());
		}

		br.close();
		f.close();
	}

	
	
	/*
	 * checks if a character is upper case in the provided string
	 */
	public static boolean isUpperCase(String str) {
		String regEx = "([A-Z])";
		Pattern pattern = Pattern.compile(regEx);
		Matcher matcher = pattern.matcher(str);
		return matcher.find();
	}

	/*
	 * Finds the similarity between strings.
	 */
	public static boolean isMatch(String str1, String str2, int type) {
		if (str1 == null || str2 == null) {
			return false;
		}
		if (str1.equals("") || str2.equals("")) {
			return false;
		}

		// str1 = str1.toLowerCase();
		// str2 = str2.toLowerCase();
		if (type == Constants.ONEWORD) {

			String[] words1 = str1.split(" ");
			String[] words2 = str2.split(" ");

			if (str1.equals(str2)) {
				return true;
			}
			if (words1.length == 1) {
				return str2.contains(words1[0]);
			} else if (words2.length == 1) {
				return str1.contains(words2[0]);
			} else {
				Set<String> wordSet1 = new HashSet<String>(
						Arrays.asList(words1));
				Set<String> wordSet2 = new HashSet<String>(
						Arrays.asList(words2));

				boolean set1IsLarger = wordSet1.size() > wordSet2.size();
				Set<String> cloneSet = new HashSet<String>(
						set1IsLarger ? wordSet2 : wordSet1);
				cloneSet.retainAll(set1IsLarger ? wordSet1 : wordSet2);
				return (!cloneSet.isEmpty());
			}
		} else if (type == Constants.HARD_MATCH) {
			if (str1.equalsIgnoreCase(str2)) {
				return true;
			}
		} else if (type == Constants.JACCARD) {
			String[] words1 = str1.split(" ");
			String[] words2 = str2.split(" ");
			Set<String> wordSet1 = new HashSet<String>(Arrays.asList(words1));
			Set<String> wordSet2 = new HashSet<String>(Arrays.asList(words2));
			Set<String> intersection = new HashSet<String>(wordSet1);
			Set<String> union = new HashSet<String>(wordSet1);
			intersection.retainAll(wordSet2);
			union.addAll(wordSet2);
			if (intersection.size() / union.size() > Constants.JACCARD_THRESH)
				return true;
		} else if (type == Constants.SUBSTRING) {
			if (str2.contains(str1) || str1.contains(str2)) {
				return true;
			}

		}

		return false;
	}

	/*
	 * Gets key for value
	 */
	public static String getKeyForValue(Map<String, String> map, String value) {
		for (String key : map.keySet()) {
			if (map.get(key).equalsIgnoreCase(value)) {
				return key;
			}
		}
		return null;
	}

	/*
	 * Invert a one-one map
	 */
	public static Map<String, Integer> invertNEDict(Map<Integer, String> dict) {
		Map<String, Integer> invertedDict = new HashMap<String, Integer>(
				(int) (dict.size() / 0.75) + 1);

		for (Integer key : dict.keySet()) {
			invertedDict.put(dict.get(key), key);
		}
		return invertedDict;
	}

	/*
	 * Invert a one-one map
	 */
	public static Map<String, String> invertDict(Map<String, Set<String>> dict) {
		Map<String, String> invertedDict = new HashMap<String, String>(
				(int) (dict.size() / 0.75) + 1);

		for (String key : dict.keySet()) {
			for (String val : dict.get(key)) {
				invertedDict.put(val, key);
			}

		}
		return invertedDict;
	}

	/*
	 * convert Matts's Dictionary data structure to Map
	 */
	public static Map<String, String> convertDictToMap(Dictionary dict) {
		Map<String, String> convMap = new HashMap<String, String>();
		int size = dict.getNextIndex();
		for (int i = 0; i < size; i++) {
			convMap.put(Integer.toString(i), dict.getKey(i));
		}
		return convMap;
	}

	public static Map<Integer, String> appendStringBuilderToDict(
			Map<Integer, String> newMap, StringBuilder data) {
		String[] lines = data.toString().split("\n");

		for (int i = 0; i < lines.length; i++) {
			if (lines[i].length() > 1) {
				String[] words = lines[i].split("\t");
				if (words.length > 1) {
					newMap.put(Integer.parseInt(words[0]), words[1]);
				}
			}
		}
		return newMap;
	}

	public static Map<String, Integer> appendStringBuilderToInverseDict(
			Map<String, Integer> newMap, StringBuilder data) {
		String[] lines = data.toString().split("\n");

		for (int i = 0; i < lines.length; i++) {
			if (lines[i].length() > 1) {
				String[] words = lines[i].split("\t");
				if (words.length > 1) {
					newMap.put(words[1], Integer.parseInt(words[0]));
				}
			}
		}
		return newMap;
	}

	public static Map<String, Set<String>> appendStringBuilderToInvAliases(
			Map<String, Set<String>> newMap, StringBuilder data) {
		String[] lines = data.toString().split("\n");

		for (int i = 0; i < lines.length; i++) {
			if (lines[i].length() > 1) {
				String[] words = lines[i].split("\t");
				if (newMap.containsKey(words[1])) {
					newMap.get(words[1]).add(words[0]);
				} else {
					newMap.put(words[1], new HashSet<String>());
					newMap.get(words[1]).add(words[0]);
				}

			}
		}
		return newMap;
	}

	public static Map<String, Set<String>> appendStringBuilderToMap(
			Map<String, Set<String>> newMap, StringBuilder data,
			boolean isInvert) {
		String[] lines = data.toString().split("\n");

		for (int i = 0; i < lines.length; i++) {
			if (lines[i].length() > 1) {
				String[] words = lines[i].split("\t");
				Set<String> temp = new HashSet<String>();
				if (isInvert) {
					temp.add(words[1]);
					newMap.put(words[0], temp);
				} else {
					temp.add(words[0]);
					newMap.put(words[1], temp);
				}
			}
		}

		return newMap;
	}

	/*
	 * Delete sharding files
	 */
	public static void deleteShards(String dirName) {
		File graphDir = new File(dirName);
		String[] files = graphDir.list();
		for (int i = 0; i < files.length; i++) {
			if (new File(graphDir + "/" + files[i]).isFile()) {
				if (!files[i].equalsIgnoreCase("edges.tsv")) {
					FileUtils
							.deleteQuietly(new File(graphDir + "//" + files[i]));
				}
			}
		}
	}
}
