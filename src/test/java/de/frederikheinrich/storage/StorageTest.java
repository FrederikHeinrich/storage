package de.frederikheinrich.storage;

import com.mongodb.ConnectionString;
import de.flapdoodle.embed.mongo.commands.MongodArguments;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.embed.process.io.ProcessOutput;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.transitions.Start;
import de.frederikheinrich.watchable.Watchable;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StorageTest {


    private static ConnectionString connectionString;

    private final static String REPLICA_SET = "replicaSet";

    private static List<TransitionWalker.ReachedState<RunningMongodProcess>> servers = new ArrayList<>();

    private static Net net(String host) {
        return Net.builder()
                .from(Net.defaults())
                .bindIp(host)
                .build();
    }

    private static TransitionWalker.ReachedState<RunningMongodProcess> initServer(IFeatureAwareVersion version) {
        TransitionWalker.ReachedState<RunningMongodProcess> server = Mongod.instance().withNet(Start.to(Net.class).initializedWith(net("localhost")))
                .withProcessOutput(Start.to(ProcessOutput.class).initializedWith(ProcessOutput.silent()))
                .withMongodArguments(Start.to(MongodArguments.class).initializedWith(
                        MongodArguments.defaults().withArgs(Map.of("--replSet", REPLICA_SET))
                                .withUseSmallFiles(true).withUseNoJournal(false))).start(version);
        servers.add(server);
        return server;
    }

    @BeforeAll
    public static void setUp() {
//        for (int i = 0; i < 3; i++) {
//            initServer(Version.Main.V7_0);
//        }
//
//        Document rs = new Document();
//        rs.append("_id", REPLICA_SET);
//        List<Document> members = new ArrayList<>();
//        AtomicInteger i = new AtomicInteger();
//        servers.forEach(process -> {
//            members.add(new Document().append("_id", i.getAndIncrement()).append("host", process.current().getServerAddress().toString()));
//        });
//        rs.append("members", members);
//
//        String master = servers.get(0).current().getServerAddress().toString();
//
//
//        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString("mongodb://" + master)).build();
//        try (MongoClient mongo = MongoClients.create(settings)) {
//            MongoDatabase adminDb = mongo.getDatabase("admin");
//            Document cr = adminDb.runCommand(new Document("isMaster", 1));
//            System.out.println("isMaster: " + cr.get("ismaster"));
//            System.out.println("info: " + cr.get("info"));
//
//            cr = adminDb.runCommand(new Document("replSetInitiate", rs));
//            System.out.println("replSetInitiate: " + cr);
//            AtomicBoolean isReady = new AtomicBoolean(false);
//            while (!isReady.get()) {
//                Document result = adminDb.runCommand(new Document("replSetGetStatus", 1));
//                //  System.out.println("replSetGetStatus: " + result);
//                isReady.set(isReplicaSetStarted(result));
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException ignored) {
//                }
//            }
//
//        }
//        System.out.println("Server Count: " + servers.size());
//        servers.forEach(process -> {
//            System.out.println(" - " + process.current().getServerAddress().toString());
//        });
//
//
//        String uri = "mongodb://";
//        uri = uri + String.join(",", servers.stream().map(runningMongodProcessReachedState -> runningMongodProcessReachedState.current().getServerAddress().toString()).toList());
//        uri = uri + "/?replicaSet=" + REPLICA_SET;
//        connectionString = new ConnectionString(uri);
        //storage = new Storage(connectionString.toString());
        storage = new Storage("mongodb+srv://user:c1IaxuPTWBrt9ZCh@cluster0.vlfjnrk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0");
    }

    @Test
    public void testSynced() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
    }


//    @AfterAll
//    public static void stopMongoDatabase() {
//        try {
//            Thread.sleep(500);
//        } catch (InterruptedException ignored) {
//        }
//        System.out.println("Closing MongoDB");
//        storage.databases.forEach(database -> database.collections.forEach(collection -> {
//            collection.list().thenAccept(objects -> {
//                System.out.println("Database: " + database + " , Collection: " + collection.collection.getNamespace() + ", Items: " + objects.toString());
//            });
//        }));
//        storage.close();
//        servers.forEach(TransitionWalker.ReachedState::close);
//    }

    private static boolean isReplicaSetStarted(Document setting) {
        if (!setting.containsKey("members")) {
            return false;
        }

        @SuppressWarnings("unchecked")
        List<Document> members = setting.get("members", List.class);
        for (Document member : members) {
            //System.out.println("replica set member " + member);
            int state = member.getInteger("state");
            //System.out.println("state: " + state);
            // 1 - PRIMARY, 2 - SECONDARY, 7 - ARBITER
            if (state != 1 && state != 2 && state != 7) {
                return false;
            }
        }
        return true;
    }

    @AfterEach
    @BeforeEach
    public void dropTables() {
        //MongoDatabase database = client.getDatabase(SCHEMA_NAME);
        //   database.listCollectionNames().forEach(c -> database.getCollection(c).drop());
    }

    private static Storage storage;

    @Test
    public void startUpStorage() {
        Database database = storage.getDatabase("testDB");
        assertNotNull(database, "Database fetching failed");
        SyncedCollection<TestElement> collection = database.syncedCollection(TestElement.class);
        collection.list().join().forEach(testElement -> {
            System.out.println("Adding watcher on " + testElement.id);
            testElement.text.on((s, t1) -> {
                System.out.println("CHANGED : " + testElement.id + " -> " + s + " to " + t1);
            });
        });

        assertNotNull(collection, "Collection fetching failed");

        TestElement element = new TestElement("TEST");

        collection.add(element).thenAccept(aBoolean -> {
            if (aBoolean.wasAcknowledged()) {
                System.out.println("Inserted test element - " + aBoolean.getInsertedId());
            } else {
                System.out.println("Insert Failed");
            }
            collection.list().thenAccept(testElements -> {
                testElements.stream().filter(testElement -> testElement.text.equals(element.text)).findFirst().ifPresentOrElse(testElement -> {
                    assertEquals(element.text, testElement.text);
                }, () -> {

                });
            });
        }).join();
        ;
        collection.list().thenAccept(testRecords -> {
            System.out.println("List: " + testRecords);
            System.out.println("Count: " + testRecords.size());
            testRecords.forEach(System.out::println);
        });
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TestElement {

        @BsonId
        public ObjectId id;

        public Watchable<String> text = Watchable.watch("unset");
        public String text2;
        public String test3;

        public TestElement(String text) {
            this.text = Watchable.watch(text);
            this.text2 = text;
            this.test3 = text + text;
        }

        public String getText() {
            return text.get();
        }

        public void setText(String text) {
            this.text.set(text);
        }

        public String getText2() {
            return text2;
        }

        public void setText2(String text2) {
            this.text2 = text2;
        }

        public TestElement() {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestElement element = (TestElement) o;
            return Objects.equals(id, element.id);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public String toString() {
            return "TestElement{" +
                    "id=" + id +
                    ", text=" + text +
                    ", text2='" + text2 + '\'' +
                    ", test3='" + test3 + '\'' +
                    '}';
        }
    }
}
