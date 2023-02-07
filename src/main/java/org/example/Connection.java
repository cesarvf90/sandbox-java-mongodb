package org.example;

import com.mongodb.ConnectionString;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Connection {

    public static void main(String[] args) {
        ConnectionString connectionString = new ConnectionString("mongodb+srv://mongodb:mongodb@cluster0.szp1aqs.mongodb.net/?retryWrites=true&w=majority");
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            List<Document> databases = mongoClient.listDatabases().into(new ArrayList<>());
//            databases.forEach(db -> System.out.println(db.toJson()));

            MongoDatabase database = mongoClient.getDatabase("blog");
            MongoCollection<Document> collection = database.getCollection("posts");

//            insertOneDocument(collection);

            findDocumentsAgeGreaterThan(collection, 25);

//            updateOneDocument(collection);

//            deleteOneDocument(collection);

//            runTransaction(mongoClient);

            runAggregationMatch(collection);

            runAggregationMatchAndCount(collection);

            runAggregationSortAndProject(collection);

        }

    }

    private static void runAggregationSortAndProject(MongoCollection<Document> collection) {
        System.out.println("Aggregation example - Sort and Project");
        Bson matchStage = Aggregates.match(Filters.gte("age", 20));
        Bson sortStage = Aggregates.sort(Sorts.orderBy(Sorts.ascending("age")));
        Bson projectStage = Aggregates.project(Projections.fields(Projections.include("name", "age"), Projections.excludeId()));
        collection.aggregate(Arrays.asList(matchStage, sortStage, projectStage)).forEach(document -> System.out.println(document.toJson()));
    }

    private static void runAggregationMatch(MongoCollection<Document> collection) {
        System.out.println("Aggregation example - Match");
        Bson matchStage = Aggregates.match(Filters.gte("age", 20));
        collection.aggregate(Arrays.asList(matchStage)).forEach(item -> System.out.println(item.toJson()));
    }

    private static void runAggregationMatchAndCount(MongoCollection<Document> collection) {
        System.out.println("Aggregation example - Match and Count");
        Bson matchStage = Aggregates.match(Filters.gte("age", 20));
        Bson groupStage = Aggregates.group("subscriptionType", Accumulators.avg("averageAge", "$age"));
        collection.aggregate(Arrays.asList(matchStage, groupStage)).forEach(document -> System.out.println(document.toJson()));
    }

    private static void runTransaction(MongoClient mongoClient) {
        ClientSession clientSession = mongoClient.startSession();
        TransactionBody transactionBody = new TransactionBody() {
            @Override
            public Object execute() {
                MongoCollection<Document> postsCollection = mongoClient.getDatabase("blog").getCollection("posts");
                Document document1 = new Document("_id", new ObjectId())
                        .append("name", "Cesar")
                        .append("age", "32");
                Document document2 = new Document("_id", new ObjectId())
                        .append("name", "Daniela")
                        .append("age", "30");
                postsCollection.insertOne(document1);
                postsCollection.insertOne(document2);
                return "Added 2 people";
            }
        };

        try {
            clientSession.withTransaction(transactionBody);
        } catch (RuntimeException e) {
            System.out.println(e);
        } finally {
            clientSession.close();
        }
    }

    private static void deleteOneDocument(MongoCollection<Document> collection) {
        Bson query = Filters.eq("name", "Alicia");
        DeleteResult result = collection.deleteOne(query);
        System.out.println(result);
    }

    private static void updateOneDocument(MongoCollection<Document> collection) {
        Bson query = Filters.eq("_id", new ObjectId("63c552b65759734278188ab7"));
//        Bson query = Filters.eq("name", "Antonia");
        Bson update = Updates.set("age", 38);
        UpdateResult result = collection.updateOne(query, update);
        System.out.println(result);
    }

    private static void findDocumentsAgeGreaterThan(MongoCollection<Document> collection, int age) {
        collection.find(Filters.and(Filters.gte("age", age))).forEach(doc -> System.out.println(doc.toJson()));
        Document document = collection.find(Filters.and(Filters.gte("age", age))).first();
        System.out.println(document.toJson());
    }

    private static void insertOneDocument(MongoCollection<Document> collection) {
        Document post = new Document("_id", new ObjectId())
                .append("name", "Eduardo")
                .append("age", "19");

        InsertOneResult result = collection.insertOne(post);
        BsonValue id = result.getInsertedId();
        System.out.println("Inserted ID: " + id);
    }
}
