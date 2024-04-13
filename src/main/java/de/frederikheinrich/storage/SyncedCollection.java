package de.frederikheinrich.storage;

import com.mongodb.client.model.changestream.FullDocument;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SyncedCollection<T> extends Collection<T> {

    private ArrayList<T> local = new ArrayList<>();

    private Field id;

    private Thread thread;

    public SyncedCollection(Database database, Class<T> element) {
        super(database, element);
        for (Field field : element.getDeclaredFields()) {
            System.out.println(element.getSimpleName() + " -> " + field.getName());
            if (field.isAnnotationPresent(BsonId.class)) {
                id = field;
            }
        }
        if (id == null)
            throw new RuntimeException(element.getSimpleName() + " is missing a id");

        collection.find().forEach(t -> local.add(t));
        System.out.println("SYNCED COLLECTION");
        System.out.println("Local size: " + local.size());
        thread = new Thread(() -> {
            try {
                collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).forEach(change -> {
                    System.out.println(change);
                    T full = change.getFullDocument();
                    ObjectId id = change.getDocumentKey().get("_id").asObjectId().getValue();
                    System.out.println("ID: " + id);
                    System.out.println("Full: " + full);

                    switch (change.getOperationType()) {
                        case INSERT -> {
                            local.add(full);
                        }
                        case DELETE -> {
                            local.removeIf(t -> t.equals(full));
                        }
                        case UPDATE -> {
                            local.stream().filter(t -> t.equals(full)).findFirst().ifPresentOrElse(t -> {
                                assert change.getUpdateDescription() != null;
                                assert change.getUpdateDescription().getUpdatedFields() != null;
                                change.getUpdateDescription().getUpdatedFields().forEach((s, bsonValue) -> {
                                    try {
                                        Object newValue = t.getClass().getMethod("get" + s.substring(0, 1).toUpperCase() + s.substring(1)).invoke(full);
                                        t.getClass().getMethod("set" + s.substring(0, 1).toUpperCase() + s.substring(1), newValue.getClass()).invoke(t, newValue);
                                    } catch (NoSuchMethodException | InvocationTargetException |
                                             IllegalAccessException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                            }, () -> {
                                System.out.println("ERROR UPDATE");
                            });
                        }
                        case DROP, DROP_DATABASE -> local.clear();
                        case REPLACE -> {
                            local.removeIf(t -> full.equals(t));
                            local.add(full);
                        }
                        case RENAME -> {
                            System.out.println("Renaming " + change.getFullDocumentBeforeChange() + " to " + change.getFullDocument());
                        }
                        case OTHER -> {
                            System.out.println("OTHER " + change.getFullDocumentBeforeChange() + " to " + change.getFullDocument());
                        }
                    }
                });
            } catch (IllegalStateException ignored) {
            }
        });
        thread.start();
    }

    @Override
    public CompletableFuture<List<T>> list() {
        return CompletableFuture.supplyAsync(() -> new ArrayList<T>(local));
    }

    @Override
    protected void close() {
        System.out.println("Closing collection");
        thread.interrupt();
    }

    @Override
    public String toString() {
        return "SyncedCollection{" +
                "collection=" + collection +
                ", element=" + element +
                ", thread=" + thread +
                ", local=" + local +
                '}';
    }
}
