import org.neo4j.driver.v1.*;

import java.util.*;
import java.util.stream.Collectors;

import org.dom4j.Document;
import org.dom4j.io.SAXReader;
import org.dom4j.Element;
import org.dom4j.Node;


import static org.neo4j.driver.v1.Values.parameters;

public class DirectoryGovAu implements AutoCloseable {
    private final Driver driver;

    public DirectoryGovAu(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    //CREATE CONSTRAINT ON (n:board) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:commonwealth_of_parliament) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:courts) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:directory_role) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:directory_sub_structure) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:enquiry_line) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:governor_general) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:non_board) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:organisation) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:person) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:portfolio) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:portfolio_role) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:role) ASSERT n.content_id IS UNIQUE;
//CREATE CONSTRAINT ON (n:single_executive_role) ASSERT n.content_id IS UNIQUE;
    public static void main(String... args) throws Exception {
        SAXReader reader = new SAXReader();
        Document document = reader.read("export.xml");
        HashSet<String> checkedRelationships = new HashSet<>();
        HashSet<String> relationships = new HashSet<>(Arrays.asList("portfolio", "contact", "role_belongs_to",
                "parent_organisation", "parent_directory_structure", "parent_board_non_board", "parent_non_portfolio"));

        try (DirectoryGovAu dga = new DirectoryGovAu("bolt://localhost:7687", "neo4j", "password")) {
            // create nodes
            document.getRootElement().elements().stream().forEach(item -> {
                if (item.getName() != null && item.getName().equals("item")) {
                    Map<String, Value> map = item.elements().stream()
                            .collect(Collectors.<Element, String, Value>toMap(n -> n.getName(), n -> Values.value(n.getText())));

                    System.out.println(map.get("content_id") + ": " + map.get("title"));
                    System.out.println("---------");
                    dga.createNode(map.get("type").asString(), map);
                    System.out.println();
                }
            });
            // parse nodes for relationships
            document.getRootElement().elements().stream().forEach(item -> {
                if (item.getName() != null && item.getName().equals("item")) {
                    item.elements().parallelStream().forEach(tag -> {
                        String relationship = tag.getName();
                        if (relationships.contains(relationship)) {
                            if (!checkedRelationships.contains(relationship + tag.getText())) {
                                System.out.println(relationship + " --> " + tag.getText());
                                dga.createRelationship(relationship, tag.getText());
                                checkedRelationships.add(relationship + tag.getText());
                            }
                        }
                    });
                }
            });
        }
    }


    @Override
    public void close() throws Exception {
        driver.close();
    }

    public void createNode(final String type, final Map<String, Value> values) {
        try (Session session = driver.session()) {
            session.writeTransaction(new TransactionWork<StatementResult>() {
                @Override
                public StatementResult execute(Transaction tx) {
                    StatementResult result = tx.run("MERGE (a:" + type + " " +
                                    "{" +
                                    values.keySet().stream()
                                            .map(n -> n + ":$" + n)
                                            .collect(Collectors.joining(",")) +
                                    "})",
                            Values.value((Object) values));
                    return result;
                }
            });
        }
    }

    public void createRelationship(final String type, final String id) {
        try (Session session = driver.session()) {
            session.writeTransaction(new TransactionWork<StatementResult>() {
                @Override
                public StatementResult execute(Transaction tx) {
                    StatementResult result = tx.run("match (a {content_id: $id}), " +
                                    "(b {" + type + ": $id}) MERGE (a)<-[r:" + type + "]-(b) " +
                                    " return a.title,type(r),b.title",
                            parameters("id", id));
                    return result;
                }
            });
        }
    }
}