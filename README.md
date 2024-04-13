# storage

[![](https://jitpack.io/v/FrederikHeinrich/storage.svg)](https://jitpack.io/#FrederikHeinrich/storage)

MongoDB Storage System is a Java library that provides a flexible and asynchronous storage system for managing collections of objects in MongoDB.

## Usage

### Installation

Add the dependency to MongoDB Storage System to your Maven or Gradle configuration:

Maven:

```xml

<dependency>
    <groupId>com.github.FrederikHeinrich</groupId>
    <artifactId>storage</artifactId>
    <version>Tag</version>
</dependency>
```

Gradle:

```groovy
implementation 'com.github.FrederikHeinrich:storage:Tag'
```

Replace `Tag` with the version tag of the release you want to use.

### Examples

Create a database and a collection:

```java
String uri = "mongodb://localhost:27017";
Storage storage = new Storage(uri);
Database database = storage.getDatabase("my_database");
Collection<MyObject> collection = database.collection(MyObject.class);
```

Perform operations on the collection:

```java
MyObject obj = new MyObject();
collection.add(obj);

collection.list().thenAccept(objects -> {
    for (MyObject o : objects) {
        System.out.println(o);
    }
});

collection.remove(obj);
```

### Notes

- All operations are asynchronous, utilizing CompletableFuture for non-blocking execution.
- The SyncedCollection class automatically synchronizes local collections with changes in the MongoDB database in real-time.

## Contributing

If you find a bug or have an enhancement suggestion, please create an issue or pull request on GitHub.