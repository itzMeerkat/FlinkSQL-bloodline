package me.jiayizhang.graghgen;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class GraphGenerator {
    int TASK_SIZE = 10;
    int DATA_SIZE = 50;


    public HashMap<String, GraphNode> map = new HashMap<>();
    public ArrayList<GraphEdge> edges = new ArrayList<>();

    public void AddSinkRelation(String task, String rel) {
        if (!map.containsKey(task)) {
            map.put(task, new GraphNode(task, "Task", TASK_SIZE));
        }
        if (!map.containsKey(rel)) {
            map.put(rel, new GraphNode(rel, "Data", DATA_SIZE));
        }
        edges.add(new GraphEdge(task, rel));
    }

    public void AddSourceRelation(String task, String rel) {
        if (!map.containsKey(task)) {
            map.put(task, new GraphNode(task, "Task", TASK_SIZE));
        }
        if (!map.containsKey(rel)) {
            map.put(rel, new GraphNode(rel, "Data", DATA_SIZE));
        }
        edges.add(new GraphEdge(rel, task));
    }

    public String GenerateGraph() {
        class GraphOutput {
            public Collection<GraphNode> Nodes;
            public Collection<GraphEdge> Edges;
            GraphOutput(Collection<GraphNode> n, Collection<GraphEdge> e) {
                Nodes = n;
                Edges = e;
            }
        }
        GraphOutput opt = new GraphOutput(map.values(), edges);
        Gson gson = new Gson();
        return gson.toJson(opt);
    }
}
