package de.frederikheinrich.storage;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.ArrayList;

public class Storage {


    protected MongoClient client;
    protected ArrayList<Database> databases = new ArrayList<>();

    public Storage(String uri) {
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
        CodecRegistry pojoCodecRegistry = MongoClientSettings.getDefaultCodecRegistry();
        CodecRegistry customCodecRegistry = CodecRegistries.fromProviders(pojoCodecProvider);
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(pojoCodecRegistry, customCodecRegistry);

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .codecRegistry(codecRegistry)
                .build();

        this.client = MongoClients.create(settings);

    }

    public Database getDatabase(String database) {
        return new Database(this, database);
    }

    public void close() {
        databases.forEach(Database::close);
        client.close();
    }


}
