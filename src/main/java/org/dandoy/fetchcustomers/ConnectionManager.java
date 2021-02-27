package org.dandoy.fetchcustomers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionManager {
    public static Connection createConnection() throws SQLException {
        try (FileInputStream fileInputStream = new FileInputStream(new File(System.getProperty("user.home"), "sqlserver.properties"))) {
            final Properties properties = new Properties();
            properties.load(fileInputStream);
            final String url = properties.getProperty("url");
            final String username = properties.getProperty("username");
            final String password = properties.getProperty("password");
            return DriverManager.getConnection(url, username, password);
        } catch (IOException e) {
            throw new IllegalStateException("Failed", e);
        }
    }
}
