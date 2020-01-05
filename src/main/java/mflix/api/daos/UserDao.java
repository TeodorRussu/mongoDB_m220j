package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Optional;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    public static final String EMAIL = "email";
    public static final String USER_ID = "user_id";
    private final MongoCollection<User> usersCollection;
    private final MongoCollection<Document> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        sessionsCollection = db.getCollection("sessions");
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */

    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {

        if (email == null) {
            throw new IncorrectDaoOperation("email is null");
        }
        if (userPreferences == null) {
            throw new IncorrectDaoOperation("preferences object is null");
        }

        boolean updated = false;
        Document userPreferencesDocument = new Document();
        userPreferencesDocument.putAll(userPreferences);

        try {
            usersCollection.updateMany(eq(EMAIL, email), set("preferences", userPreferencesDocument));
            updated = true;
        } catch (IncorrectDaoOperation ex) {
            ex.printStackTrace();
        }
        return updated;
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        try {
            usersCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(user);
            return true;
        }
        catch (MongoWriteException exception){
            log.error("user already exists");
            throw new IncorrectDaoOperation("user already exists");
        }
    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {

        Document session = new Document();
        session.append(USER_ID, userId);
        session.append("jwt", jwt);


        if (Optional.ofNullable(sessionsCollection.find(eq(USER_ID, userId)).first()).isPresent()) {
            sessionsCollection.updateOne(eq(USER_ID, userId), set("jwt", jwt));
        } else {
            sessionsCollection.insertOne(session);
        }
        return true;
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        return usersCollection.find(new Document(EMAIL, email)).iterator().tryNext();
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {

        Document queryFilter = new Document(USER_ID, userId);
        Document actual = sessionsCollection.find(queryFilter).limit(1).iterator().tryNext();
        if (actual == null)
            return null;
        Session session = new Session();
        session.setJwt(actual.getString("jwt"));
        session.setUserId(actual.getString(USER_ID));
        return session;
    }

    public boolean deleteUserSessions(String userId) {

        Document sessionToDelete = new Document(USER_ID, userId);
        return sessionsCollection.deleteMany(sessionToDelete).wasAcknowledged();
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        // remove user sessions
        Document userToDelete = new Document(EMAIL, email);
        deleteUserSessions(email);
        return usersCollection.deleteMany(userToDelete).wasAcknowledged();
    }
}
