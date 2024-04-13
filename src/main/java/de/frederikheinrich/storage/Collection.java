package de.frederikheinrich.storage;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.result.InsertOneResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Collection<T> {

    protected Class<T> element;
    protected MongoCollection<T> collection;

    public Collection(Database database, Class<T> element) {
        database.collections.add(this);
        this.element = element;
        this.collection = database.database.getCollection(element.getSimpleName(), element);
        System.out.println("Collection created: " + collection.getNamespace());

    }

    public CompletableFuture<List<T>> list() {
        return CompletableFuture.supplyAsync(() -> {
            var list = new ArrayList<T>();
            collection.find().forEach(list::add);
            return list;
        });
    }

    public CompletableFuture<InsertOneResult> add(T element) {
        return CompletableFuture.supplyAsync(() -> {
            var insert = collection.insertOne(element, new InsertOneOptions());
            System.out.println("Added " + element + " - " + insert);
            return insert;
        });
    }

    public CompletableFuture<Boolean> remove(T element) {
        return CompletableFuture.supplyAsync(() -> {
            return collection.deleteOne(Filters.eq(element)).wasAcknowledged();
        });
    }

    public CompletableFuture<Void> clear() {
        return CompletableFuture.supplyAsync(() -> {
            collection.drop();
            return null;
        });
    }

    protected void close() {

    }

    @Override
    public String toString() {
        return "Collection{" +
                "element=" + element +
                ", collection=" + collection +
                '}';
    }
}
