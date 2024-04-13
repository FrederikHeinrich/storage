package de.frederikheinrich.storage;

import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;

public class Database {

    protected MongoDatabase database;
    protected ArrayList<Collection<?>> collections = new ArrayList<>();

    public Database(Storage storage, String database) {
        this.database = storage.client.getDatabase(database);
        storage.databases.add(this);
    }

    public <T> Collection<T> collection(Class<T> element) {
        return new Collection<T>(this, element);
    }

    public <T> SyncedCollection<T> syncedCollection(Class<T> element) {
        return new SyncedCollection<>(this, element);
    }

    public void close() {
        collections.forEach(Collection::close);
    }

    @Override
    public String toString() {
        return "Database{" +
                "database=" + database +
                ", collections=" + collections +
                '}';
    }
}
