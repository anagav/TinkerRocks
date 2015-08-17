package com.tinkerrocks.structure;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by ashishn on 8/16/15.
 */
public class RocksTest1 {

    RocksGraph graph;
    JsonParser jsonParser;

    @Before
    public void setup() throws IOException {
        Configuration configuration = new BaseConfiguration();
        FileUtils.forceMkdir(new File("/tmp/databases"));
        graph = new RocksGraph(configuration);
        jsonParser = new JsonParser();
    }


    @Test
    public void testActualData() throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("/tmp/out"));
        String line;
        JsonObject object;
        graph.createIndex("height", Vertex.class);
        graph.createIndex("name", Vertex.class);
        int count = 0;
        while ((line = bufferedReader.readLine()) != null) {
            count++;
            object = jsonParser.parse(line).getAsJsonObject();
            String id = object.get("@id").getAsString();
            Vertex v = graph.vertices(T.id).hasNext() ? graph.vertices(T.id).next() : graph.addVertex(T.id, id, T.label, "person");

            String label;
            if (object.has("label")) {
                label = object.get("label").getAsString();
                v.property("name", label);
            }

            float height;
            if (object.has("height")) {
                height = object.get("height").getAsFloat();
                v.property("height", height);
            }
            //  graph.addVertex(T.id, id, T.label, "person", "name", label, "height", height);
        }
        System.out.println("inserted records:" + count);
    }


    @After
    public void close() throws Exception {
        this.graph.close();
    }

}
