package de.frederikheinrich.storage;

import com.mongodb.client.MongoDatabase;

import java.util.concurrent.ConcurrentHashMap;

public class Database {

    protected Storage storage;
    protected MongoDatabase database;
    protected ConcurrentHashMap<String, Collection<?>> collections = new ConcurrentHashMap<>();
    protected ConcurrentHashMap<String, SyncedCollection<?>> syncedCollections = new ConcurrentHashMap<>();

    protected Database(Storage storage, String database) {
        this.storage = storage;
        this.database = storage.client.getDatabase(database);
        storage.databases.put(database, this);
    }

    public MongoDatabase mongo() {
        return database;
    }

    public <T> Collection<T> collection(Class<T> element) {
        if (collections.containsKey(element.getSimpleName().toLowerCase()))
            return (Collection<T>) collections.get(element.getSimpleName());
        return new Collection<T>(this, element);
    }

    public <T> SyncedCollection<T> syncedCollection(Class<T> element) {
        if (syncedCollections.containsKey(element.getSimpleName().toLowerCase())) {
            return (SyncedCollection<T>) syncedCollections.get(element.getSimpleName().toLowerCase());
        }
        return new SyncedCollection<>(this, element);
    }

    public void close() {
        collections.forEach((s, collection) -> {
            collection.close();
            collections.remove(s);
        });
    }

    @Override
    public String toString() {
        return "Database{" +
                "database=" + database +
                ", collections=" + collections +
                '}';
    }
}
