package org.jfu.test.neo4j;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

public class GraphServiceImpl implements GraphService {

    private GraphDatabaseService graphDatabaseService;

    private Index<Node> index;

    public GraphServiceImpl(String neoStoreDir) {
        graphDatabaseService = new GraphDatabaseFactory()
                .newEmbeddedDatabase(neoStoreDir);

        try (Transaction tx = graphDatabaseService.beginTx()) {
            index = graphDatabaseService.index().forNodes("index");
            tx.success();
        }
    }

    @Override
    public void destroy() {
        graphDatabaseService.shutdown();
    }

    private Node findNodeFromIndex(Integer userId) {
        IndexHits<Node> hits = index.query("id", userId);
        if (hits.hasNext()) {
            return hits.getSingle();
        }
        return null;
    }

    private Node findOrCreateNode(Integer userId) {
        Node node = findNodeFromIndex(userId);
        if (node == null) {
            node = graphDatabaseService.createNode();
            node.setProperty("id", userId);

            index.add(node, "id", userId);
        }
        return node;
    }

    @Override
    public void makeFriend(Integer userId, Integer targetId) {
        try (Transaction tx = graphDatabaseService.beginTx()) {
            Node user = findOrCreateNode(userId);
            Node target = findOrCreateNode(targetId);
            user.createRelationshipTo(target, RelType.KNOWS);
            tx.success();
        }
    }

    @Override
    public Set<Integer> getFriendAtDistance(Integer userId, int distance) {
        try (Transaction tx = graphDatabaseService.beginTx()) {
            Set<Integer> friends = new HashSet<Integer>();

            Node user = findNodeFromIndex(userId);

            if (user != null) {
                TraversalDescription td = graphDatabaseService
                        .traversalDescription().breadthFirst()
                        .relationships(RelType.KNOWS, Direction.BOTH)
                        .evaluator(Evaluators.fromDepth(distance))
                        .evaluator(Evaluators.toDepth(distance))
                        .uniqueness(Uniqueness.NODE_GLOBAL);
                for (Node node : td.traverse(user).nodes()) {
                    Integer friendId = (Integer) node.getProperty("id");
                    friends.add(friendId);
                }
            }
            tx.success();

            return friends;
        }
    }

    @Override
    public Integer distance(Integer userId, Integer targetId, int maxDistance) {
        try (Transaction tx = graphDatabaseService.beginTx()) {
            Node user = findNodeFromIndex(userId);
            Node target = findNodeFromIndex(targetId);

            if (user == null || target == null) {
                return null;
            }

            PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                    PathExpanders.forTypeAndDirection(RelType.KNOWS,
                            Direction.BOTH), maxDistance);
            Path path = finder.findSinglePath(user, target);

            Integer distance = null;
            if (path != null) {

                distance = path.length();
            }
            tx.success();

            return distance;
        }
    }

    @Override
    public List<List<Integer>> shortestPath(Integer userId, Integer targetId,
            int maxDistance, int maxPaths) {
        try (Transaction tx = graphDatabaseService.beginTx()) {
            Node user = findNodeFromIndex(userId);
            Node target = findNodeFromIndex(targetId);

            if (user == null || target == null) {
                return null;
            }

            PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                    PathExpanders.forTypeAndDirection(RelType.KNOWS,
                            Direction.BOTH), maxDistance);

            int count = 0;
            List<List<Integer>> shortestPaths = new LinkedList<List<Integer>>();
            for (Path path : finder.findAllPaths(user, target)) {

                List<Integer> shortestPath = new LinkedList<Integer>();
                for (Node node : path.nodes()) {
                    shortestPath.add((Integer)node.getProperty("id"));
                }
                shortestPaths.add(shortestPath);
                count++ ;

                if (count >= maxPaths) {
                    break;
                }
            }

            tx.success();

            return shortestPaths;
        }
    }

}
