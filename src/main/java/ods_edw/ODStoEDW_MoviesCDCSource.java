package ods_edw;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class ODStoEDW_MoviesCDCSource {

    private static final String RESUME_TOKEN_COLLECTION = "resume_tokens";
    private static final String RESUME_TOKEN_DOCUMENT_ID = "movies_cdc_resume_token";

    public static void main(String... args) throws Exception {

        // Retrieve mongoUri from environment variable
        String mongoUri = System.getenv("ConnectionString");
        if (mongoUri == null || mongoUri.isEmpty()) {
            throw new IllegalArgumentException("ConnectionString environment variable is not set.");
        }

        String PROJECT_ID = System.getenv("PROJECT_ID");
        if (PROJECT_ID == null || PROJECT_ID.isEmpty()) {
            throw new IllegalArgumentException("PROJECT_ID environment variable is not set.");
        }

        String topicId = System.getenv("TOPIC_ID");
        if (topicId == null || topicId.isEmpty()) {
            throw new IllegalArgumentException("TOPIC_ID environment variable is not set.");
        }

        String DATABASE = System.getenv("DATABASE");
        if (DATABASE == null || DATABASE.isEmpty()) {
            throw new IllegalArgumentException("DATABASE environment variable is not set.");
        }

        String COLLECTION = System.getenv("COLLECTION");
        if (COLLECTION == null || COLLECTION.isEmpty()) {
            throw new IllegalArgumentException("COLLECTION environment variable is not set.");
        }

        System.out.println("PROJECT_ID: " + PROJECT_ID);
        System.out.println("topicId: " + topicId);

        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        MongoDatabase database = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> collection = database.getCollection(COLLECTION);

        // Resume token collection
        MongoCollection<Document> resumeTokenCollection = database.getCollection(RESUME_TOKEN_COLLECTION);

        // Try to retrieve the resume token from the collection
        Document resumeTokenDoc = resumeTokenCollection.find(new Document("_id", RESUME_TOKEN_DOCUMENT_ID)).first();
        org.bson.BsonDocument resumeToken = null;
        if (resumeTokenDoc != null && resumeTokenDoc.containsKey("token")) {
            resumeToken = resumeTokenDoc.get("token", org.bson.BsonDocument.class);
            System.out.println("Found resume token: " + resumeToken);
        }

        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topicId);
        List<ApiFuture<String>> futures = new ArrayList<>();
        final Publisher publisher = Publisher.newBuilder(topicName).build();

        Block<ChangeStreamDocument<Document>> printBlock = new Block<ChangeStreamDocument<Document>>() {
            @Override
            public void apply(final ChangeStreamDocument<Document> changeStreamDocument) {

                String messageData;

                // Determine the operation type and construct the message accordingly
                if (changeStreamDocument.getOperationType() == OperationType.UPDATE) {
                    System.out.println("Update operation detected for document ID: " + changeStreamDocument.getDocumentKey());
                    if (changeStreamDocument.getFullDocument() != null) {
                        messageData = changeStreamDocument.getFullDocument().toJson();
                    } else {
                        messageData = "{\"_id\":" + changeStreamDocument.getDocumentKey().toJson() + ", \"operationType\":\"UPDATE\", \"updatedFields\":" + changeStreamDocument.getUpdateDescription().getUpdatedFields().toJson() + "}";
                        return;
                    }
                } else if (changeStreamDocument.getOperationType() == OperationType.INSERT ||
                           changeStreamDocument.getOperationType() == OperationType.REPLACE) {
                    System.out.println("Insert/Replace operation detected for document ID: " + changeStreamDocument.getDocumentKey());
                    messageData = changeStreamDocument.getFullDocument().toJson();
                } else if (changeStreamDocument.getOperationType() == OperationType.DELETE) {
                    System.out.println("Delete operation detected for document ID: " + changeStreamDocument.getDocumentKey());
                    messageData = "{\"_id\":" + changeStreamDocument.getDocumentKey().toJson() + ", \"operationType\":\"DELETE\"}";
                    System.out.println("Publishing message: " + messageData);
                    return;
                } else {
                    System.out.println("Other operation detected: " + changeStreamDocument.getOperationType());
                    messageData = changeStreamDocument.toString();
                    return;
                }

                try {
                    System.out.println("Publishing message: " + messageData);
                    ByteString data = ByteString.copyFromUtf8(messageData);
                    PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                            .setData(data)
                            .build();
                    ApiFuture<String> future = publisher.publish(pubsubMessage);
                    futures.add(future);

                    // Save the resume token after processing each event
                    org.bson.BsonDocument token = changeStreamDocument.getResumeToken();
                    if (token != null) {
                        Document tokenDoc = new Document("_id", RESUME_TOKEN_DOCUMENT_ID)
                                .append("token", token);
                        resumeTokenCollection.replaceOne(
                                new Document("_id", RESUME_TOKEN_DOCUMENT_ID),
                                tokenDoc,
                                new com.mongodb.client.model.UpdateOptions().upsert(true)
                        );
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        // Build the change stream iterable with or without resume token
        com.mongodb.client.ChangeStreamIterable<Document> changeStreamIterable;
        changeStreamIterable = collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
        if (resumeToken != null) {
            changeStreamIterable = changeStreamIterable.resumeAfter(resumeToken);
        }

        changeStreamIterable.forEach(printBlock);
    }
}
