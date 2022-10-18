package com.hackathon.tinker.demo.api;

import com.hackathon.tinker.demo.SearchRequest;
import com.hackathon.tinker.demo.server.TinkerGraphTempServer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
public class SearchController {

    TinkerGraphTempServer server;
    Graph graph;
    GraphTraversalSource g;

    SearchController(TinkerGraphTempServer tinkerGraphTempServer) {
        this.server = tinkerGraphTempServer;
        this.g = tinkerGraphTempServer.getG();
        this.graph = tinkerGraphTempServer.getGraph();
    }


    @GetMapping("/search/{id}")
    @ResponseBody
    public String search(@PathVariable Integer id) throws SerializationException {

        Vertex vertex = g.V().has("SB_WF", "nodeId",id).next();
        List<Vertex> vertices = g.V(vertex).both("Fav").toList();
        vertices.add(vertex);

        //for directed output
        List<Edge> edges = g.V(vertex).bothE("Fav").toList();
        edges.forEach(edge-> System.out.println("v1 = "+edge.inVertex().property("nodeId") +" " +
                "  v2   "+edge.outVertex().property("nodeId")));

        return createResponse(vertices, edges);
    }



    @PostMapping("/search")
    @ResponseBody
    public String search(@RequestBody SearchRequest searchRequest) throws SerializationException {
        GraphTraversal<Vertex, Vertex> graphTraversal =  g.V().limit(500).hasLabel("SB_WF");
        if(searchRequest.getEntityType() != null){
            graphTraversal = graphTraversal.has("entityType",searchRequest.getEntityType());
        }
        if(searchRequest.getState()!= null){
            graphTraversal = graphTraversal.has("state",searchRequest.getState());
        }


        List<Vertex> vertices   = graphTraversal.toList();
        List<Object> edges = new ArrayList<>();

        if(!vertices.isEmpty()){
            edges = g.V(vertices).aggregate("SB_WF")
                    .bothE().as("Fav").inV()
                    .where(P.within("SB_WF"))
                    .select("Fav").toList();
        }


        return createResponse(vertices, edges);

    }

    private String createResponse(List<Vertex> vertices, List<?> edges) throws SerializationException {
        List<Object> data = new ArrayList<>();
        data.add(vertices);
        data.add(edges);

        final UUID requestId = UUID.randomUUID();

        final ByteBuf response = convertToByteBuf(data, requestId);
        String resultString = response.toString(Charset.forName("utf-8"));
        return resultString;
    }


    private ByteBuf convertToByteBuf(final Object toSerialize, UUID requestId) throws SerializationException {
        MessageSerializer<ObjectMapper> serializer = new GraphSONMessageSerializerV3d0();
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ResponseMessage.Builder responseMessageBuilder = ResponseMessage.build(requestId);

        final ByteBuf bb = serializer.serializeResponseAsBinary(responseMessageBuilder.result(toSerialize).create(), allocator);
        return bb;
    }
}
