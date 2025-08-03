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
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class ODStoEDW_MoviesCDCSource {

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
                    // For updates, you might want to send the updated fields (updateDescription)
                    // along with the full document, or just the full document after the update.
                    // Here, we'll send the full document for simplicity, but you could customize.
                    if (changeStreamDocument.getFullDocument() != null) {
                        messageData = changeStreamDocument.getFullDocument().toJson();
                    } else {
                        // If full document is not available for update (e.g., if configured not to send it)
                        // you might want to send the updated fields from getUpdateDescription()
                        messageData = "{\"_id\":" + changeStreamDocument.getDocumentKey().toJson() + ", \"operationType\":\"UPDATE\", \"updatedFields\":" + changeStreamDocument.getUpdateDescription().getUpdatedFields().toJson() + "}";
                        return;
                    }
                } else if (changeStreamDocument.getOperationType() == OperationType.INSERT ||
                           changeStreamDocument.getOperationType() == OperationType.REPLACE) {
                    System.out.println("Insert/Replace operation detected for document ID: " + changeStreamDocument.getDocumentKey());
                    messageData = changeStreamDocument.getFullDocument().toJson();
                } else if (changeStreamDocument.getOperationType() == OperationType.DELETE) {
                    System.out.println("Delete operation detected for document ID: " + changeStreamDocument.getDocumentKey());
                    // For deletes, getFullDocument() will be null, so send the document key.
                    messageData = "{\"_id\":" + changeStreamDocument.getDocumentKey().toJson() + ", \"operationType\":\"DELETE\"}";
                    System.out.println("Publishing message: " + messageData);
                    return;
                } else {
                    // For other operation types like DROP, RENAME, etc.
                    System.out.println("Other operation detected: " + changeStreamDocument.getOperationType());
                    messageData = changeStreamDocument.toString(); // Send the entire change stream document as a string
                    return;
                }

                try {
                    System.out.println("Publishing message: " + messageData);
                    ByteString data = ByteString.copyFromUtf8(messageData);
                    PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                            .setData(data)
                            .build();
                    // Schedule a message to be published. Messages are automatically batched.
                    ApiFuture<String> future = publisher.publish(pubsubMessage);
                    futures.add(future);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {

                }
            }
        };

        /**
         * Change stream listener
         */
        collection.watch()
            .fullDocument(FullDocument.UPDATE_LOOKUP)
            .forEach(printBlock);

    }
}
