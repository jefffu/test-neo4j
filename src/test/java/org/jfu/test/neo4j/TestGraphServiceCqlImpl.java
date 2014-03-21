package org.jfu.test.neo4j;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestGraphServiceCqlImpl {
    private static Logger logger = LoggerFactory.getLogger(TestGraphServiceCqlImpl.class);

    private GraphService graphService;
    private String neoStoreDir = "neoStore";

    @Before
    public void before() {
        graphService = new GraphServiceCqlImpl(neoStoreDir);
        buildNetwork();
    }

    /**
     *  1--------+
     *  |        |
     *  v        v
     *  2     +->5---+
     *  |    /       |
     *  v   /        v
     *  3-->6   +--->8
     *  |      /     |
     *  v     /      v
     *  4--->7       9
     */
    private void buildNetwork() {
        graphService.makeFriend(1, 2);
        graphService.makeFriend(1, 5);
        graphService.makeFriend(2, 3);
        graphService.makeFriend(3, 4);
        graphService.makeFriend(3, 6);
        graphService.makeFriend(4, 7);
        graphService.makeFriend(5, 8);
        graphService.makeFriend(6, 5);
        graphService.makeFriend(7, 8);
        graphService.makeFriend(8, 9);
    }

    @Test
    public void test() {
        long cost = System.currentTimeMillis();
        test1stDegreeFriends();
        logger.debug("test1stDegreeFriends() cost "+(System.currentTimeMillis() - cost)+" ms.");

        test2ndDegreeFriends();
        logger.debug("test2ndDegreeFriends() cost "+(System.currentTimeMillis() - cost)+" ms.");

        test3rdDegreeFriends();
        logger.debug("test3rdDegreeFriends() cost "+(System.currentTimeMillis() - cost)+" ms.");

        testDistance();
        logger.debug("testDistance() cost "+(System.currentTimeMillis() - cost)+" ms.");

        testShortestPath();
        logger.debug("testShortestPath() cost "+(System.currentTimeMillis() - cost)+" ms.");
    }

    private void test1stDegreeFriends() {
        Set<Integer> friendsAt1stDegree = graphService.getFriendAtDistance(1, 1);
        logger.debug("======== 1st degree friends of 1: " + friendsAt1stDegree);
        Assert.assertTrue(friendsAt1stDegree.contains(2));
        Assert.assertTrue(friendsAt1stDegree.contains(5));

        Assert.assertFalse(friendsAt1stDegree.contains(1));
        Assert.assertFalse(friendsAt1stDegree.contains(3));
        Assert.assertFalse(friendsAt1stDegree.contains(4));
        Assert.assertFalse(friendsAt1stDegree.contains(6));
        Assert.assertFalse(friendsAt1stDegree.contains(7));
        Assert.assertFalse(friendsAt1stDegree.contains(8));
        Assert.assertFalse(friendsAt1stDegree.contains(9));
    }

    private void test2ndDegreeFriends() {
        Set<Integer> friendsAt2ndDegree = graphService.getFriendAtDistance(1, 2);
        logger.debug("======== 2nd degree friends of 1: " + friendsAt2ndDegree);
        Assert.assertTrue(friendsAt2ndDegree.contains(3));
        Assert.assertTrue(friendsAt2ndDegree.contains(6));
        Assert.assertTrue(friendsAt2ndDegree.contains(8));

        Assert.assertFalse(friendsAt2ndDegree.contains(1));
        Assert.assertFalse(friendsAt2ndDegree.contains(2));
        Assert.assertFalse(friendsAt2ndDegree.contains(4));
        Assert.assertFalse(friendsAt2ndDegree.contains(5));
        Assert.assertFalse(friendsAt2ndDegree.contains(7));
        Assert.assertFalse(friendsAt2ndDegree.contains(9));
    }

    private void test3rdDegreeFriends() {
        Set<Integer> friendsAt3rdDegree = graphService.getFriendAtDistance(1, 3);
        logger.debug("======== 3rd degree friends of 1: " + friendsAt3rdDegree);
        Assert.assertTrue(friendsAt3rdDegree.contains(4));
        Assert.assertTrue(friendsAt3rdDegree.contains(7));
        Assert.assertTrue(friendsAt3rdDegree.contains(9));

        Assert.assertFalse(friendsAt3rdDegree.contains(1));
        Assert.assertFalse(friendsAt3rdDegree.contains(2));
        Assert.assertFalse(friendsAt3rdDegree.contains(3));
        Assert.assertFalse(friendsAt3rdDegree.contains(5));
        Assert.assertFalse(friendsAt3rdDegree.contains(6));
        Assert.assertFalse(friendsAt3rdDegree.contains(8));
    }

    private void testDistance() {
        Integer distance = graphService.distance(1, 6, 3);
        Assert.assertEquals("Distance between 1 and 6", new Integer(2), distance);

        distance = graphService.distance(1, 4, 3);
        Assert.assertEquals("Distance between 1 and 3", new Integer(3), distance);

        distance = graphService.distance(2, 9, 3);
        Assert.assertNull("Distance between 2 and 9", distance);
    }

    private void testShortestPath() {
        List<List<Integer>> shortestPath = graphService.shortestPath(2, 8, 3, 5);
        Assert.assertEquals("Has only one shortest path between 2 and 8", 1, shortestPath.size());
        logger.debug("The shortest path between 2 and 8: " + shortestPath.get(0));
    }


    @After
    public void after() {
        graphService.destroy();
        File file = new File(neoStoreDir);
        delete(file);
    }

    private void delete(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                delete(f);
            }
        } else {
            file.delete();
        }
    }

}
