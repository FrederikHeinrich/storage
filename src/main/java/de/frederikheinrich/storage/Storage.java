package de.frederikheinrich.storage;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.concurrent.ConcurrentHashMap;

public class Storage {


    protected MongoClient client;
    protected ConcurrentHashMap<String, Database> databases = new ConcurrentHashMap<>();


    /**
     * The Storage class is responsible for managing the connection to a MongoDB database
     * and providing access to the various collections within the database.
     * It provides methods to retrieve a specific database, close the connection,
     * and perform operations on collections.
     * <p>
     * This Storage() constructor makes use of the system environment variable "MONGO_URI" to
     * establish the connection to a MongoDB server. Ensure that the "MONGO_URI" environment variable
     * is properly set.
     */
    public Storage(String uri) {
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
        CodecRegistry pojoCodecRegistry = MongoClientSettings.getDefaultCodecRegistry();
        CodecRegistry customCodecRegistry = CodecRegistries.fromProviders(pojoCodecProvider);
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(pojoCodecRegistry, customCodecRegistry);

        MongoClientSettings settings = MongoClientSettings.builder()
                .uuidRepresentation(UuidRepresentation.JAVA_LEGACY)
                .applyConnectionString(new ConnectionString(uri))
                .codecRegistry(codecRegistry)
                .build();
        this.client = MongoClients.create(settings);
    }

    public MongoClient mongo() {
        return client;
    }

    public boolean isReady() {
        return client != null;
    }

    public Database getDatabase(String database) {
        if (databases.containsKey(database))
            return databases.get(database);
        return new Database(this, database);
    }

    public void close() {
        databases.forEach((s, database) -> {
            database.close();
            databases.remove(s);
        });
        client.close();
    }

}
