package consumer_dead;

import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Database {
    public static Connection connectToDatabase() throws Exception {
        Connection connection = null;

        Class.forName("org.postgresql.Driver");
        String host = "localhost";
        String port = "5433";
        String database = "postgres";
        String user = "postgres";
        String password = "postgres";

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        connection = DriverManager.getConnection(url, user, password);

        return connection;
    }
}