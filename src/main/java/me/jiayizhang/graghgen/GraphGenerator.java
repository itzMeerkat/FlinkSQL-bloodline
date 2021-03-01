package me.jiayizhang.graghgen;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GraphGenerator {
    int TASK_SIZE = 10;
    int DATA_SIZE = 50;


    public HashMap<String, GraphNode> map = new HashMap<>();
    public ArrayList<GraphEdge> edges = new ArrayList<>();
    public HashMap<String, Integer> degreeCnt = new HashMap<>();

    public void AddSinkRelation(String task, String rel) {
        if (rel.equals("print")) {
            return;
        }
        if (!map.containsKey(task)) {
            map.put(task, new GraphNode(task, "Task", TASK_SIZE));
        }
        if (!map.containsKey(rel)) {
            map.put(rel, new GraphNode(rel, "Data", DATA_SIZE));
        }
        degreeCnt.put(task, degreeCnt.getOrDefault(task, 0) + 1);
        degreeCnt.put(rel, degreeCnt.getOrDefault(rel, 0) + 1);
        edges.add(new GraphEdge(task, rel));
    }

    public void AddSourceRelation(String task, String rel) {
        if (!map.containsKey(task)) {
            map.put(task, new GraphNode(task, "Task", TASK_SIZE));
        }
        if (!map.containsKey(rel)) {
            map.put(rel, new GraphNode(rel, "Data", DATA_SIZE));
        }
        degreeCnt.put(task, degreeCnt.getOrDefault(task, 0) + 1);
        degreeCnt.put(rel, degreeCnt.getOrDefault(rel, 0) + 1);
        edges.add(new GraphEdge(rel, task));
    }

    public String GenerateGraph() {
        HashMap<String, Collection> j = new HashMap<>();
        for (GraphNode x: map.values()) {
            x.Size = degreeCnt.get((x.Label));
        }
        j.put("nodes", map.values());
        j.put("edges", edges);
        Gson gson = new Gson();
        return gson.toJson(j);
    }
}
