package de.frederikheinrich.storage;

import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class SyncedCollection<T> extends Collection<T> {

    protected final ArrayList<T> local = new ArrayList<>();

    protected final ArrayList<Consumer<T>> onInserts = new ArrayList<>();

    protected final Thread thread = new Thread(() -> {
        try {
            collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).forEach(change -> {
                T full = change.getFullDocument();
                OperationType operation = change.getOperationType();
                switch (operation) {
                    case INSERT:
                        local.add(full);
                        onInserts.forEach(consumer -> consumer.accept(full));
                        break;
                    case DELETE:
                        local.removeIf(t -> t.equals(full));
                        break;
                    case UPDATE:
                        local.stream().filter(t -> t.equals(full)).findFirst().ifPresentOrElse(t -> {
                            assert change.getUpdateDescription() != null;
                            assert change.getUpdateDescription().getUpdatedFields() != null;
                            change.getUpdateDescription().getUpdatedFields().forEach((s, bsonValue) -> {
                                try {
                                    Object newValue = t.getClass().getMethod("get" + s.substring(0, 1).toUpperCase() + s.substring(1)).invoke(full);
                                    t.getClass().getMethod("set" + s.substring(0, 1).toUpperCase() + s.substring(1), newValue.getClass()).invoke(t, newValue);
                                } catch (NoSuchMethodException | InvocationTargetException |
                                         IllegalAccessException e) {
                                    try {
                                        Field field = element.getField(s);
                                        field.setAccessible(true);
                                        field.set(t, field.get(full));
                                    } catch (NoSuchFieldException | IllegalAccessException ex) {
                                        throw new RuntimeException(ex);
                                    }
                                }
                            });
                        }, () -> {
                            throw new RuntimeException("Updated missing local Element?\n" + change);
                        });
                        break;
                    case DROP:
                    case DROP_DATABASE:
                        local.clear();
                        break;
                    case REPLACE:
                        local.removeIf(t -> {
                            assert full != null;
                            return full.equals(t);
                        });
                        local.add(full);
                        break;
                    case RENAME:
                        System.out.println("Renaming " + change.getFullDocumentBeforeChange() + " to " + change.getFullDocument());
                        break;
                    case OTHER:
                        System.out.println("OTHER " + change.getFullDocumentBeforeChange() + " to " + change.getFullDocument());
                        break;
                    default:
                        System.out.println("Unknown operation type: " + change.getOperationType());
                        break;
                }
            });
        } catch (IllegalStateException ignored) {
        }
    });

    public void onInsert(Consumer<T> onInsert) {
        onInserts.add(onInsert);
    }

    protected SyncedCollection(Database database, Class<T> element) {
        super(database, element);
        database.syncedCollections.put(element.getSimpleName().toLowerCase(), this);
        collection.find().forEach(local::add);
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
