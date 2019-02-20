package com.serli.oracle.of.bacon.repository;

import java.util.LinkedList;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Path;

public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        this.driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "ab65ed24c4"));
    }

    public List<GraphItem> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();

        Transaction transaction = session.beginTransaction();

        StatementResult statementResult = transaction.run("MATCH (kb:Actors {name: 'Bacon, Kevin (I)'}), (ac:Actors {name: {actorName}}), sp = shortestPath((kb)-[:PLAYED_IN*]-(ac)) RETURN sp;",
                                                        Values.parameters("actorName", actorName));

        LinkedList<GraphItem> graph = new LinkedList<>();
        Record record;
        List<Value> recordValues;
        Path path;
        
        while (statementResult.hasNext()) {
        	record = statementResult.next();
        	recordValues = record.values();
        	
        	for (Value val : recordValues) {
        		path = val.asPath();
        		
        		path.nodes().forEach(node -> {
        			String type = node.labels().iterator().next();
        			String keyNodeProperty = "Actors".equals(type) ? "name" : "title";
        			graph.add(new GraphNode(node.id(), node.get(keyNodeProperty).asString(), type));
        		});
        		
        		path.relationships().forEach(relationship -> {
        			graph.add(new GraphEdge(relationship.id(), relationship.startNodeId(), relationship.endNodeId(), relationship.type()));
        		});
        	}
        }
        
        return graph;
    }

    public static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
        
        /**
         * Pour suivre le bon format à exposer dans l'APIEndPoint
         */
        @Override
        public String toString() {
        	StringBuilder strBuilder = new StringBuilder();
        	strBuilder.append("\n{ \"data\" : { ")
        			  .append("\n	    \"id\": "     ).append(this.id   ).append(", "  )
        			  .append("\n     \"type\": \"" ).append(this.type ).append("\", ")
        			  .append("\n     \"value\": \"").append(this.value).append("\"")
        			  .append("\n}}")
        			  ;
        	return strBuilder.toString();
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
        
        /**
         * Pour suivre le bon format à exposer dans l'APIEndPoint
         */
        @Override
        public String toString() {
        	StringBuilder strBuilder = new StringBuilder();
        	strBuilder.append("\n{ \"data\" : { ")
        			  .append("\n	    \"id\": "     ).append(this.id    ).append(", "  )
        			  .append("\n     \"source\": " ).append(this.source).append(", "  )
        			  .append("\n     \"target\": " ).append(this.target).append(", "  )
        			  .append("\n     \"value\": \"").append(this.value ).append("\"")
        			  .append("\n}}")
        			  ;
        	return strBuilder.toString();
        }
    }
}
