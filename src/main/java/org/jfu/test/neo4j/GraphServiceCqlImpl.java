package org.jfu.test.neo4j;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class GraphServiceCqlImpl implements GraphService {

    private GraphDatabaseService graphDatabaseService;
    private ExecutionEngine engine;

    public GraphServiceCqlImpl(String neoStoreDir) {

        graphDatabaseService = new GraphDatabaseFactory()
                .newEmbeddedDatabase(neoStoreDir);
        engine = new ExecutionEngine(graphDatabaseService);
    }

    @Override
    public void destroy() {
        graphDatabaseService.shutdown();
    }

    @Override
    public void makeFriend(Integer userId, Integer targetId) {
        try (Transaction tx = graphDatabaseService.beginTx()) {
            engine.execute("MERGE ({ id: " + userId + " })");
            engine.execute("MERGE ({ id: " + targetId + " })");
            engine.execute("MATCH (user { id: " + userId
                    + " }), (target { id: " + targetId
                    + " }) CREATE UNIQUE (user)-[:" + RelType.KNOWS.name()
                    + "]->(target)");
            tx.success();
        }
    }

    @Override
    public Set<Integer> getFriendAtDistance(Integer userId, int distance) {
        Set<Integer> friends = new HashSet<Integer>();
        if (distance <= 0) {
            return friends;
        }
        try (Transaction tx = graphDatabaseService.beginTx()) {
            ExecutionResult result = engine.execute("MATCH (user { id: "
                    + userId + " })-[:" + RelType.KNOWS.name() + "*" + distance
                    + "]-(friend) WHERE NOT (user)-[:" + RelType.KNOWS.name()
                    + "*.." + (distance - 1) + "]-(friend) RETURN friend.id");
            ResourceIterator<Long> ri = result.columnAs("friend.id");
            while (ri.hasNext()) {
                Long friendId = ri.next();
                if (friendId != null) {
                    friends.add(friendId.intValue());
                }
            }
            tx.success();
        }
        return friends;
    }

    @Override
    public Integer distance(Integer userId, Integer targetId, int maxDistance) {
        Integer distance = null;
        try (Transaction tx = graphDatabaseService.beginTx()) {
            ExecutionResult result = engine.execute("MATCH (user { id: "
                    + userId + "}), (target { id: " + targetId
                    + "}), p=shortestPath((user)-[:" + RelType.KNOWS.name()
                    + "*.."+maxDistance+"]-(target)) return p");
            ResourceIterator<Path> ri = result.columnAs("p");
            if (ri.hasNext()) {
                Path path = ri.next();
                distance = path.length();
            }
        }
        return distance;
    }

    @Override
    public List<List<Integer>> shortestPath(Integer userId, Integer targetId,
            int maxDistance, int maxPaths) {
        int count = 0;
        List<List<Integer>> shortestPaths = new LinkedList<List<Integer>>();
        try (Transaction tx = graphDatabaseService.beginTx()) {
            ExecutionResult result = engine.execute("MATCH (user { id: "
                    + userId + "}), (target { id: " + targetId
                    + "}), p=allShortestPaths((user)-[:" + RelType.KNOWS.name()
                    + "*.."+maxDistance+"]-(target)) return p");
            ResourceIterator<Path> ri = result.columnAs("p");
            while (ri.hasNext()) {
                Path path = ri.next();
                List<Integer> shortestPath = new LinkedList<Integer>();
                for (Node node : path.nodes()) {
                    Long id = (Long)node.getProperty("id");
                    shortestPath.add(id.intValue());
                }
                shortestPaths.add(shortestPath);
                count++ ;

                if (count >= maxPaths) {
                    break;
                }
            }
        }
        return shortestPaths;
    }

}
