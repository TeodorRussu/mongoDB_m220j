package mflix;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.connection.SslSettings;
import org.bson.Document;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);

    String welcomeMessage =
        ""
            + "\n"
            + " __          __  _                            _          __  __ ______ _ _      \n"
            + " \\ \\        / / | |                          | |        |  \\/  |  ____| (_)     \n"
            + "  \\ \\  /\\  / /__| | ___ ___  _ __ ___   ___  | |_ ___   | \\  / | |__  | |___  __\n"
            + "   \\ \\/  \\/ / _ \\ |/ __/ _ \\| '_ ` _ \\ / _ \\ | __/ _ \\  | |\\/| |  __| | | \\ \\/ /\n"
            + "    \\  /\\  /  __/ | (_| (_) | | | | | |  __/ | || (_) | | |  | | |    | | |>  < \n"
            + "     \\/  \\/ \\___|_|\\___\\___/|_| |_| |_|\\___|  \\__\\___/  |_|  |_|_|    |_|_/_/\\_\\\n"
            + "                                                                                \n"
            + "                                                                                \n"
            + "     ^\n"
            + "   /'|'\\\n"
            + "  / \\|/ \\\n"
            + "  | \\|/ |\n"
            + "   \\ | /\n"
            + "    \\|/\n"
            + "     |\n"
            + "                       \n";
    System.out.println(welcomeMessage);
  }
}
