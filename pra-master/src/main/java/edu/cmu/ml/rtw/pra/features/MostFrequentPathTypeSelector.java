package edu.cmu.ml.rtw.pra.features;

import java.util.List;
import java.util.Map;

import edu.cmu.ml.rtw.users.matt.util.MapUtil;

public class MostFrequentPathTypeSelector implements PathTypeSelector {

	//@Override
	public List<PathType> selectPathTypes(Map<PathType, Integer> pathCounts,
			int numPathsToKeep) {
		System.out
				.println("SELECTING PATH TYPES - MostFrequentPathTypeSelector");
		// Added by Bhushan
		
		for (PathType path : pathCounts.keySet()) {
			// Check repetitions and remove paths with repeated rels. Relation
			// ID 520 is
			// exempt.
			String pathString = path.toString();
			String[] relations = pathString.split("-");
			String prevrel = "";
			int counter = 0;

			for (int i = 0; i < relations.length; i++) {
				if (!relations[i].equals("") && !relations[i].contains("520")
						&& !relations[i].contains("generalizations")) {
					String strippedRel = relations[i].replace("_", "");
					if (strippedRel.equalsIgnoreCase(prevrel)) {
						counter++;
						if (counter >= 1) {
							// Remove path by making count 0
							pathCounts.put(path, 0);
							break;
						}

					} else {
						prevrel = strippedRel;
						counter = 0;
					}
				}

			}

		}

		if (numPathsToKeep == -1) {
			return MapUtil.getKeysSortedByValue(pathCounts, true);
		}
		return MapUtil.getTopKeys(pathCounts, numPathsToKeep);
	}
}
