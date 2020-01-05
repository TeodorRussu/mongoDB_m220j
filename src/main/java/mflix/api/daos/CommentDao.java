package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import lombok.extern.slf4j.Slf4j;
import mflix.api.models.Comment;
import mflix.api.models.Critic;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.descending;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Component
public class CommentDao extends AbstractMFlixDao {

    public static final String COUNT = "count";
    public static final String COMMENT_COLLECTION = "comments";
    private final Logger log;
    private MongoCollection<Comment> commentCollection;
    private CodecRegistry pojoCodecRegistry;

    @Autowired
    public CommentDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        log = LoggerFactory.getLogger(this.getClass());
        this.db = this.mongoClient.getDatabase(MFLIX_DATABASE);
        this.pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        this.commentCollection =
                db.getCollection(COMMENT_COLLECTION, Comment.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Returns a Comment object that matches the provided id string.
     * @param id - comment identifier
     * @return Comment object corresponding to the identifier value
     */
    public Comment getComment(String id) {
        return commentCollection.find(new Document("_id", new ObjectId(id))).first();
    }

    /**
     * Adds a new Comment to the collection. The equivalent instruction in the mongo shell would be:
     *
     * <p>db.comments.insertOne({comment})
     *
     * <p>
     * @param comment - Comment object.
     * @throw IncorrectDaoOperation if the insert fails, otherwise returns the resulting Comment object.
     */
    public Comment addComment(Comment comment) {

        if (comment.getId() == null)
            throw new IncorrectDaoOperation("comment has no id");
        try {
            commentCollection.insertOne(comment);
        } catch (Exception e) {
            throw new IncorrectDaoOperation(e.getMessage());
        }
        return comment;
    }

    /**
     * Updates the comment text matching commentId and user email. This method would be equivalent to running the
     * following mongo shell command:
     *
     * <p>db.comments.update({_id: commentId}, {$set: { "text": text, date: ISODate() }})
     *
     * <p>
     * @param commentId - comment id string value.
     * @param text      - comment text to be updated.
     * @param email     - user email.
     * @return true if successfully updates the comment text.
     */
    public boolean updateComment(String commentId, String text, String email) {

        Document filter = new Document();
        filter.append("_id", new ObjectId(commentId));
        filter.append("email", email);
        Bson update = new Document("$set",
                new Document()
                        .append("text", text)
                        .append("date", new Date()));
        UpdateOptions options = new UpdateOptions().upsert(true);
        try {
            commentCollection.withWriteConcern(WriteConcern.MAJORITY).updateOne(filter, update, options);
            return true;
        } catch (MongoWriteException exception) {
            log.debug(exception.getMessage());
            return false;
        }

    }

    /**
     * Deletes comment that matches user email and commentId.
     * @param commentId - commentId string value.
     * @param email     - user email value.
     * @return true if successful deletes the comment.
     */
    public boolean deleteComment(String commentId, String email) {
        DeleteResult ur = commentCollection.deleteOne(
                and(
                        eq("_id", new ObjectId(commentId)),
                        eq("email", email))
        );

        return ur.getDeletedCount() > 0;
    }

    /**
     * Ticket: User Report - produce a list of users that comment the most in the website. Query the `comments`
     * collection and group the users by number of comments. The list is limited to up most 20 commenter.
     * @return List {@link Critic} objects.
     */
    public List<Critic> mostActiveCommenters() {
        List<Critic> mostActive = new ArrayList<>();
        List<Bson> pipeline = Arrays.asList(group("$email", sum(COUNT, 1L)), sort(descending(COUNT)), limit(20));
        commentCollection.withReadConcern(ReadConcern.MAJORITY).aggregate(pipeline, Critic.class).iterator().forEachRemaining(mostActive::add);

        return mostActive;
    }
}
