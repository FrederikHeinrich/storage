package de.frederikheinrich.storage;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.conversions.Bson;

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
    }

    public CompletableFuture<List<T>> list() {
        return CompletableFuture.supplyAsync(() -> {
            ArrayList<T> list = new ArrayList<T>();
            collection.find().forEach(list::add);
            return list;
        });
    }

    public CompletableFuture<InsertOneResult> add(T element) {
        return CompletableFuture.supplyAsync(() -> collection.insertOne(element, new InsertOneOptions()));
    }

    public CompletableFuture<DeleteResult> remove(T element) {
        return CompletableFuture.supplyAsync(() -> collection.deleteOne(Filters.eq(element)));
    }

    public CompletableFuture<Void> clear() {
        return CompletableFuture.supplyAsync(() -> {
            collection.drop();
            return null;
        });
    }

    public CompletableFuture<FindIterable<T>> find(Bson filters) {
        return CompletableFuture.supplyAsync(() -> collection.find(filters));
    }

    public CompletableFuture<UpdateResult> update(T obj, Bson update) {
        return CompletableFuture.supplyAsync(() -> {
            return collection.updateOne(Filters.eq(obj), update, new UpdateOptions().upsert(true));
        });
    }

    public CompletableFuture<UpdateResult> update(Bson filters, Bson update) {
        return CompletableFuture.supplyAsync(() -> {
            return collection.updateOne(Filters.eq(filters), update, new UpdateOptions().upsert(true));
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
