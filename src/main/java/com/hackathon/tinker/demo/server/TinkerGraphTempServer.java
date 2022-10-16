package com.hackathon.tinker.demo.server;

import com.hackathon.tinker.demo.api.TinkerController;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

@Component
public class TinkerGraphTempServer {
    TinkerGraph graph = TinkerGraph.open();
    GraphTraversalSource g = null;

    TinkerGraphTempServer(){
        graph.createIndex("nodeId", Vertex.class);
        g = traversal().withEmbedded(graph);
    }

    public TinkerGraph getGraph() {
        return graph;
    }

    public void setGraph(TinkerGraph graph) {
        this.graph = graph;
    }

    public GraphTraversalSource getG() {
        return g;
    }

    public void setG(GraphTraversalSource g) {
        this.g = g;
    }
}
