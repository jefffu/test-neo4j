package org.jfu.test.neo4j;

import java.util.List;
import java.util.Set;

public interface GraphService {

    public void destroy();

    public void makeFriend(Integer userId, Integer targetId);

    public Set<Integer> getFriendAtDistance(Integer userId, int distance);

    public Integer distance(Integer userId, Integer targetId, int maxDistance);

    public List<List<Integer>> shortestPath(Integer userId, Integer targetId, int maxDistance, int maxPaths);
}
