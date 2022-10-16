package com.hackathon.tinker.demo.service;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataLoad {
    public static void createNodes(GraphTraversalSource g) throws IOException {
//        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("localhost", 8182));
        BufferedReader reader = new BufferedReader(
                new FileReader("C:\\Users\\krupa\\Desktop\\gremlin\\Nodes.csv"));
        //ID	Label	Name	ADDRESS	CITY	STATE	ZIP	NAICS	CustomerSince	EntityType	Products
        int lineNumber = 0;
        String line = "";

        while ((line = reader.readLine()) != null) {
            String[] row = line.trim().split(",");
            lineNumber++;
            if (lineNumber == 1) continue;
            if (lineNumber % 100 == 00) System.out.println("Progress Node=" + lineNumber);
//            if (lineNumber > 500) break;
            List<String> rowList = Arrays.asList(row);
            int index = Integer.parseInt(rowList.get(0));
            String label = rowList.get(1);

            g.addV(label).property("nodeId", index)
                    .property("name", rowList.get(2))
                    .property("address", rowList.get(3))
                    .property("city", rowList.get(4))
                    .property("state", rowList.get(5))
                    .property("zip", rowList.get(6))
                    .property("naics", rowList.get(7))
                    .property("customerSince", rowList.get(8))
                    .property("entityType", rowList.get(9))
                    .property("products", rowList.get(10))
                    .next();
            // if you want to check either it contains some name
            //index 0 is first name, index 1 is last name, index 2 is ID
        }
    }

    public static void createEdgesFavourites(GraphTraversalSource g) throws IOException {
//        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("localhost", 8182));
        BufferedReader reader = new BufferedReader(
                new FileReader("C:\\Users\\krupa\\Desktop\\gremlin\\edgesFavourites.csv"));
        //EdgeID	Label	FromNodeId	ToNodeId	grossTx	grossTxFreq
        int lineNumber = 0;
        String line = "";

        while ((line = reader.readLine()) != null) {
            String[] row = line.trim().split(",");
            lineNumber++;
            if (lineNumber == 1) continue;
            if (lineNumber % 100 == 00) System.out.println("Progress Edge=" + lineNumber);
            List<String> rowList = Arrays.asList(row);
            int index = Integer.parseInt(rowList.get(0));
            String label = rowList.get(1);
            int FromNodeId = Integer.parseInt(rowList.get(2));
            int ToNodeId = Integer.parseInt(rowList.get(3));
            try {
                Vertex v1 = g.V().hasLabel("SB_WF").has("nodeId", FromNodeId).next();
                Vertex v2 = g.V().hasLabel("SB_WF").has("nodeId", ToNodeId).next();
                if (v1 != null && v2 != null) {
                    g.addE(label).property("edgeId", index).from(v1).to(v2)
                            .property("grossTx", rowList.get(4))
                            .property("grossTxFreq", rowList.get(5))
                            .next();
                }
            } catch (Exception e) {
                System.out.println("Couldn't find the vertices with id " + FromNodeId + "==>" + ToNodeId);

            }

            // if you want to check either it contains some name
            //index 0 is first name, index 1 is last name, index 2 is ID
        }
    }
}
