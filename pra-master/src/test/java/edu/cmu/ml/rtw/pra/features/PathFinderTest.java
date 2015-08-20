package edu.cmu.ml.rtw.pra.features;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.google.common.collect.Lists;

import edu.cmu.ml.rtw.users.matt.util.FakeRandom;
import edu.cmu.ml.rtw.users.matt.util.Index;
import edu.cmu.ml.rtw.users.matt.util.Pair;
import edu.cmu.ml.rtw.users.matt.util.TestUtil;

public class PathFinderTest extends TestCase {
    private FakePathTypeFactory factory = new FakePathTypeFactory();
    private List<Pair<Pair<Integer, Integer>, Integer>> edgesToExclude = Lists.newArrayList();
    private PathFinder finder;

    @Override
    public void setUp() {
        edgesToExclude = Lists.newArrayList();
        addEdgeToExclude(1, 2, 1, edgesToExclude);
        finder = new PathFinder("src/test/resources/edges.tsv",
                                1,
                                Arrays.asList(1),
                                Arrays.asList(2),
                                new SingleEdgeExcluder(edgesToExclude),
                                10,
                                PathTypePolicy.EVERYTHING,
                                factory);
    }

    @Override
    public void tearDown() {
    }

    public void testEcondePath() {
        Index<PathType> pathDict = finder.getPathDictionary();
        int[] expectedPathTypes = new int[factory.pathTypes.length];
        for (int i = 0; i < factory.pathTypes.length; i++) {
            expectedPathTypes[i] = pathDict.getIndex(factory.pathTypes[i]);
        }
        Path path = new Path(1, 10);
        assertTrue(Arrays.equals(expectedPathTypes, finder.encodePath(path)));
    }

    public void testEncodeWalkForCompanion() {
        int walkId = 0;
        int hopNum = 0;
        int sourceId = 0;
        boolean trackBit = true;
        int off = 0;
        long walk = PathFinder.Manager.encode(walkId, hopNum, sourceId, trackBit, off);
        int[] pathTypes = new int[]{3,5,6};
        finder.setEncodedWalkPaths(pathTypes, walkId, hopNum);
        long[] encodedWalks = finder.encodeWalkForCompanion(walk);
        assertEquals(pathTypes.length, encodedWalks.length);
        for (int i = 0; i < pathTypes.length; i++) {
            long e = PathFinder.Manager.encodeForCompanion(pathTypes[i], sourceId, trackBit, off);
            assertEquals(e, encodedWalks[i]);
        }

        // This one shouldn't have anything, because encodedWalkPaths hasn't been set for this
        // (walkId, hopNum) combination).
        walk = PathFinder.Manager.encode(walkId, hopNum+1, sourceId, trackBit, off);
        encodedWalks = finder.encodeWalkForCompanion(walk);
        assertEquals(0, encodedWalks.length);
    }

    public void testProcessSingleWalk() {
        // There are two things we're testing in this code: that the behavior of resets and walk
        // forwarding is consistent with what it should be (though that sadly depends on some
        // details about ordering of edges and random number generators...), and that the Path
        // objects are created and modified consistent with the walk resets and forwarding.
        FakeChiVertex chiVertex = new FakeChiVertex(1);
        chiVertex = new FakeChiVertex(1);
        chiVertex.addInEdge(1, 1);
        chiVertex.addInEdge(3, 2);
        chiVertex.addOutEdge(5, 2);
        chiVertex.addOutEdge(5, 1);
        chiVertex.addOutEdge(2, 1);
        Vertex vertex = new Vertex(chiVertex);

        FakeDrunkardContext context = new FakeDrunkardContext();
        FakeRandom random = new FakeRandom();

        int walkId = 0;
        int hopNum = 0;
        int sourceId = 0;
        boolean trackBit = true;
        int off = 0;
        long walk = PathFinder.Manager.encode(walkId, hopNum, sourceId, trackBit, off);

        // Test a random restart.
        random.setNextDouble(.00001);
        long newWalk = PathFinder.Manager.encode(walkId, 0, sourceId, trackBit, off);
        context.setExpectationsForReset(newWalk, true);
        finder.processSingleWalkAtVertex(walk, vertex, context, random);
        assertEquals(null, finder.getWalkPath(walkId));

        // Test a restart due to returning to the same node (in this case, a reflexive edge).
        random.setNextDouble(.9);
        random.setNextInt(0);
        newWalk = PathFinder.Manager.encode(walkId, 0, sourceId, trackBit, off);
        context.setExpectationsForReset(newWalk, true);
        finder.processSingleWalkAtVertex(walk, vertex, context, random);
        assertEquals(null, finder.getWalkPath(walkId));

        // Make sure we reset when there are too many hops in this walk.
        Path path = new Path(1, PathFinder.MAX_HOPS);
        for (int i = 0; i < 10; i++) {
            path.addHop(1, 1, false);
        }
        newWalk = PathFinder.Manager.encode(walkId, 0, sourceId, trackBit, off);
        context.setExpectationsForReset(newWalk, true);
        finder.setWalkPath(path, walkId);
        finder.processSingleWalkAtVertex(walk, vertex, context, random);
        assertEquals(null, finder.getWalkPath(walkId));

        // Now let's test some simple successes.
        random.setNextDouble(.9);
        random.setNextInt(1);
        finder.setWalkPath(null, walkId);
        newWalk = PathFinder.Manager.encode(walkId, hopNum+1, sourceId, trackBit, off);
        context.setExpectations(false, newWalk, 3, true);
        finder.processSingleWalkAtVertex(walk, vertex, context, random);
        path = new Path(1, PathFinder.MAX_HOPS);
        path.addHop(3, 2, true);
        assertEquals(path, finder.getWalkPath(walkId));

        // And one with some history in the path, just for kicks.
        path = new Path(1, PathFinder.MAX_HOPS);
        path.addHop(10, 10, true);
        finder.setWalkPath(path, walkId);
        random.setNextDouble(.9);
        random.setNextInt(2);
        newWalk = PathFinder.Manager.encode(walkId, hopNum+1, sourceId, trackBit, off);
        context.setExpectations(false, newWalk, 5, true);
        finder.processSingleWalkAtVertex(walk, vertex, context, random);
        path = new Path(1, PathFinder.MAX_HOPS);
        path.addHop(10, 10, true);
        path.addHop(5, 2, false);
        assertEquals(path, finder.getWalkPath(walkId));

        // And a reset due to an unallowed edge.
        random.setNextDouble(.9);
        random.setNextInt(4);
        finder.setWalkPath(null, walkId);
        newWalk = PathFinder.Manager.encode(walkId, 0, sourceId, trackBit, off);
        context.setExpectationsForReset(newWalk, true);
        finder.processSingleWalkAtVertex(walk, vertex, context, random);
        assertEquals(null, finder.getWalkPath(walkId));
    }

    public void testEncodeAndDecode() {
        int walkId = 23;
        int hopNum = 7;
        int sourceId = 123;
        boolean trackBit = true;
        int off = 97;
        long walk = PathFinder.Manager.encode(walkId, hopNum, sourceId, trackBit, off);
        assertEquals(walkId, PathFinder.Manager.walkId(walk));
        assertEquals(hopNum, PathFinder.Manager.hopNum(walk));
        assertEquals(sourceId, PathFinder.staticSourceIdx(walk));
        assertEquals(trackBit, PathFinder.staticTrackBit(walk));
        assertEquals(off, PathFinder.staticOff(walk));

        long newWalk = PathFinder.Manager.incrementHopNum(walk);
        assertEquals(hopNum + 1, PathFinder.Manager.hopNum(newWalk));
        newWalk = PathFinder.Manager.resetHopNum(walk);
        assertEquals(0, PathFinder.Manager.hopNum(newWalk));
        trackBit = false;
        walk = PathFinder.Manager.encode(walkId, hopNum, sourceId, trackBit, off);
        assertEquals(trackBit, PathFinder.staticTrackBit(walk));

        newWalk = PathFinder.setTrackBit(walk, true);
        assertTrue(PathFinder.staticTrackBit(newWalk));
        newWalk = PathFinder.setTrackBit(walk, false);
        assertFalse(PathFinder.staticTrackBit(newWalk));
        trackBit = true;
        walk = PathFinder.Manager.encode(walkId, hopNum, sourceId, trackBit, off);
        newWalk = PathFinder.setTrackBit(walk, true);
        assertTrue(PathFinder.staticTrackBit(newWalk));
        newWalk = PathFinder.setTrackBit(walk, false);
        assertFalse(PathFinder.staticTrackBit(newWalk));

        int pathType = 12;
        trackBit = true;
        long forCompanion = PathFinder.Manager.encodeForCompanion(pathType, sourceId, trackBit, off);
        assertEquals(pathType, PathFinder.Manager.pathType(forCompanion));
        assertEquals(sourceId, PathFinder.staticSourceIdx(forCompanion));
        assertEquals(trackBit, PathFinder.staticTrackBit(forCompanion));
        assertEquals(off, PathFinder.staticOff(forCompanion));
        trackBit = false;
        forCompanion = PathFinder.Manager.encodeForCompanion(pathType, sourceId, trackBit, off);
        assertEquals(trackBit, PathFinder.staticTrackBit(forCompanion));
    }

    public void expectAssertionErrorInEncode(final int walkId,
                                             final int hopNum,
                                             final int sourceId,
                                             final boolean trackBit,
                                             final int off) {
        TestUtil.expectError(AssertionError.class, new TestUtil.Function() {
            //@Override
            public void call() {
                PathFinder.Manager.encode(walkId, hopNum, sourceId, trackBit, off);
            }
        });
    }

    public void testEncodeErrors() {
        // Base numbers that are fine
        int walkId = 23;
        int hopNum = 7;
        int sourceId = 123;
        boolean trackBit = true;
        int off = 97;

        // Now make each one too high, in turn
        walkId = PathFinder.Manager.MAX_ENCODABLE_WALKS + 1;
        expectAssertionErrorInEncode(walkId, hopNum, sourceId, trackBit, off);
        walkId = 23;

        hopNum = PathFinder.Manager.MAX_ENCODABLE_HOPS + 1;
        expectAssertionErrorInEncode(walkId, hopNum, sourceId, trackBit, off);
        hopNum = 7;

        sourceId = PathFinder.Manager.MAX_SOURCES;
        expectAssertionErrorInEncode(walkId, hopNum, sourceId, trackBit, off);
        sourceId = 123;

        off = 132;
        expectAssertionErrorInEncode(walkId, hopNum, sourceId, trackBit, off);
        off = 97;
    }

    // If we get as input a source that wasn't in the graph, we should just ignore it.  That is,
    // say we're querying on a node that we didn't actually have when we created the graph, so we
    // had to add it to the node dict.  In that case, we should just drop the node, instead of
    // crashing, which is what the code currently does as of writing this test.
    public void testIgnoresNewSources() {
        finder = new PathFinder("src/test/resources/edges.tsv",
                                1,
                                Arrays.asList(10000),
                                Arrays.asList(20000),
                                new SingleEdgeExcluder(edgesToExclude),
                                10,
                                PathTypePolicy.EVERYTHING,
                                factory);
        // We don't care about the results, we just want to be sure that this actually runs.
        finder.execute(1);
    }

    // TODO(matt): this should go away and be replaced by a fake edge excluder.
    private void addEdgeToExclude(int source,
                                  int target,
                                  int type,
                                  List<Pair<Pair<Integer, Integer>, Integer>> edges) {
        edges.add(new Pair<Pair<Integer, Integer>, Integer>(new Pair<Integer, Integer>(source,
                                                                                       target),
                                                            type));
    }
}
