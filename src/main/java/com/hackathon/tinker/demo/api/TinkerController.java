package com.hackathon.tinker.demo.api;

import com.hackathon.tinker.demo.server.TinkerGraphTempServer;
import com.hackathon.tinker.demo.service.DataLoad;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.websocket.server.PathParam;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.Pop.all;


@RestController
public class TinkerController {

    TinkerGraphTempServer server;
    Graph graph;
    GraphTraversalSource g;

    TinkerController(TinkerGraphTempServer tinkerGraphTempServer) {
        this.server = tinkerGraphTempServer;
        this.g = tinkerGraphTempServer.getG();
        this.graph = tinkerGraphTempServer.getGraph();

        try {
            this.createNodes();
            this.createEdgesFavourites();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @GetMapping("/tinker")
    public List<String> tinker() {
        Vertex v1 = g.addV("person").property("name", "marko").next();
        Vertex v2 = g.addV("person").property("name", "stephen1").next();
        Vertex v3 = g.addV("person").property("name", "stephen2").next();
        Vertex v4 = g.addV("person").property("name", "stephen3").next();

        g.V(v1).addE("knows").to(v2).property("weight", 0.75).iterate();
        g.V(v1).addE("knows").to(v3).property("weight", 0.5).iterate();
        g.V(v1).addE("knows").to(v4).property("weight", 0.25).iterate();

        Vertex marko = g.V().has("person", "name", "marko").next();
        List<Vertex> peopleMarkoKnows = g.V().has("person", "name", "marko").out("knows").toList();
        System.out.println(peopleMarkoKnows.get(0).id() + "-" + peopleMarkoKnows.get(0).label() + "-" + peopleMarkoKnows.get(0).property("name").value());
        List<String> result = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            result.add(peopleMarkoKnows.get(i).property("name").value().toString());
        }
        return result;
    }

    @GetMapping("/createNodes")
    public String createNodes() throws IOException {
        DataLoad.createNodes(g);
        return "Success";
    }

    @GetMapping("/createEdgesFavourites")
    public String createEdgesFavourites() throws IOException {
        DataLoad.createEdgesFavourites(g);
        return "Success";
    }

    @GetMapping(path = "/getNeighboursOfNode/{nodeId}")
    public ResponseEntity<GraphTraversal> getNeighboursOfNode(@PathParam("nodeId") Integer nodeId) {
        return null;

    }

    /*
    Request
{"gremlin":"nodes = g.V().limit(50).toList();edges = g.V(nodes).aggregate('node').outE().as('edge').inV().where(within('node')).select('edge').toList();[nodes,edges]"}

    Response:
   {"requestId":"251b7e35-6f2c-441c-9eff-ed5c879e724b",
   "status":{"message":"","code":200,"attributes":{"@type":"g:Map","@value":[]}},
   "result":{"data":
   {"@type":"g:List","@value":[{"@type":"g:List",
   "@value":[{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int64","@value":1},"label":"person"}},{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int64","@value":3},"label":"software"}}]},
   {"@type":"g:List","@value":[{"@type":"g:Edge","@value":{"id":{"@type":"g:Int32","@value":9},"label":"created","inVLabel":"software","outVLabel":"person","inV":{"@type":"g:Int64","@value":3},"outV":{"@type":"g:Int64","@value":1}}}]}]},"meta":{"@type":"g:Map","@value":[]}}}
    * */
    @GetMapping(path = "/getHighLevelView")
    public String getHighLevelView() throws SerializationException {
        List<Vertex> nodes = g.V().limit(500).toList();
        List<Edge> edges= g.V(nodes).bothE("Fav").toList();

//        List<Object> edges = g.V(nodes).aggregate("node")
//                .outE().as("edge").inV()
//                .where(within("node"))
//                .select("edge").toList();

        List<Object> data = new ArrayList<>();
        data.add(nodes);
        data.add(edges);

        final UUID requestId = UUID.randomUUID();

        final ByteBuf response = convertToByteBuf(data, requestId);
        String resultString = response.toString(Charset.forName("utf-8"));
        return resultString;
    }

    /*
    Request:
    {"gremlin":"nodes = g.V().groupCount().by(label);nodesprop = g.V().valueMap().select(keys).groupCount();edges = g.E().groupCount().by(label);edgesprop = g.E().valueMap().select(keys).groupCount();[nodes.toList(),nodesprop.toList(),edges.toList(),edgesprop.toList()]"}
    Response:
{"requestId":"4e619dc5-547a-4960-99a4-1ca26d19835b",
"status":{"message":"","code":200,
"attributes":{"@type":"g:Map","@value":[]}},
"result":{"data":
{"@type":"g:List","@value":[{"@type":"g:List","@value":[{"@type":"g:Map","@value":["software",{"@type":"g:Int64","@value":1},"person",{"@type":"g:Int64","@value":1}]}]},{"@type":"g:List","@value":[{"@type":"g:Map","@value":[{"@type":"g:Set","@value":["name","age"]},{"@type":"g:Int64","@value":1},{"@type":"g:Set","@value":["name","lang"]},{"@type":"g:Int64","@value":1}]}]},{"@type":"g:List","@value":[{"@type":"g:Map","@value":["created",{"@type":"g:Int64","@value":1}]}]},{"@type":"g:List","@value":[{"@type":"g:Map","@value":[{"@type":"g:Set","@value":["weight"]},{"@type":"g:Int64","@value":1}]}]}]},"meta":{"@type":"g:Map","@value":[]}}}
    * */
    @GetMapping(path = "/getHighLevelStats")
    public String getHighLevelStats() throws SerializationException {
        List nodes = g.V().groupCount().by(T.label).toList();
        List nodesprop = g.V().valueMap().select(Column.keys).groupCount().toList();
        List edges = g.E().groupCount().by(T.label).toList();
        List edgesprop = g.E().valueMap().select(Column.keys).groupCount().toList();

        List<Object> data = new ArrayList<>();
        data.add(nodes);
        data.add(nodesprop);
        data.add(edges);
        data.add(edgesprop);

        final UUID requestId = UUID.randomUUID();

        final ByteBuf response = convertToByteBuf(data, requestId);
        String resultString = response.toString(Charset.forName("utf-8"));
        return resultString;
    }

    /*
    Request
    {"gremlin":"nodes = g.V(2).as(\"node\").both().as(\"node\").select(all,\"node\").unfold().valueMap().with(WithOptions.tokens).fold().inject(__.V(2).valueMap().with(WithOptions.tokens)).unfold()\nedges = g.V(2).bothE()\n[nodes.toList(),edges.toList()]"}
    */
//    @GetMapping(path = "/clickNode/{index}")
//    public String clickNode(@PathVariable long index) throws SerializationException{
//        List<Object> nodes = g.V(index).as("node").both().as("node")
//                .select(all,"node").unfold().valueMap().with(WithOptions.tokens).fold()
//                .inject(g.V(index).valueMap().with(WithOptions.tokens)).unfold().toList();
//
//        List<Edge> edges = g.V(index).bothE().toList();
//
//        List<Object> data = new ArrayList<>();
//        data.add(nodes);
//        data.add(edges);
//
//        final UUID requestId = UUID.randomUUID();
//
//        final ByteBuf response = convertToByteBuf(data, requestId);
//        String resultString = response.toString(Charset.forName("utf-8"));
//        return resultString;
//    }

    private ByteBuf convertToByteBuf(final Object toSerialize, UUID requestId) throws SerializationException {
        MessageSerializer<ObjectMapper> serializer = new GraphSONMessageSerializerV3d0();
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ResponseMessage.Builder responseMessageBuilder = ResponseMessage.build(requestId);

        final ByteBuf bb = serializer.serializeResponseAsBinary(responseMessageBuilder.result(toSerialize).create(), allocator);
        return bb;
    }
}
