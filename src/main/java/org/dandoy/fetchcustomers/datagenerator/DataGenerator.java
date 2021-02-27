package org.dandoy.fetchcustomers.datagenerator;

import org.dandoy.fetchcustomers.ConnectionManager;
import org.dandoy.fetchcustomers.model.Product;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

public class DataGenerator {
    private static final Pattern PATTERN = Pattern.compile("\t");
    public static final int NBR_INVOICES = 100_000;
    private static final Random R = new Random();

    public static void main(String[] args) throws Exception {
        try (Connection connection = ConnectionManager.createConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                //noinspection SqlWithoutWhere
                statement.execute("""
                        DELETE FROM invoice_details;
                        DELETE FROM invoices;
                        DELETE FROM customers;
                        """);
            }
            final long t0 = System.currentTimeMillis();
            createCustomers(connection);
            final List<Integer> customerIds = readCustomers(connection);

            createProducts(connection);
            final List<Product> products = readProducts(connection);

            createInvoices(connection, customerIds);
            final List<Integer> invoices = readInvoices(connection);

            createInvoiceDetails(connection, invoices, products);

            connection.commit();
            final long t1 = System.currentTimeMillis();
            System.out.printf("Invoices created in %dms%n", t1 - t0);
        }
    }

    private static void createInvoices(Connection connection, List<Integer> customerIds) throws SQLException {
        try (PreparedStatement invoiceStatement = connection.prepareStatement("INSERT INTO invoices(customer_id, freight, created_date, paid_date) VALUES (?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
            final LocalDateTime now = LocalDateTime.now();
            for (int i = 0; i < NBR_INVOICES; i++) {
                final LocalDateTime createdDate = now.minusHours(i);
                Timestamp paidTimestamp = null;
                if (R.nextInt(100) != 0) {
                    final LocalDateTime temp = createdDate.plusDays(1 + R.nextInt(30));
                    if (temp.isAfter(createdDate)) {
                        paidTimestamp = Timestamp.valueOf(temp);
                    }
                }

                invoiceStatement.setInt(1, customerIds.get(R.nextInt(customerIds.size())));
                invoiceStatement.setInt(2, R.nextInt(100));
                invoiceStatement.setTimestamp(3, Timestamp.valueOf(createdDate));
                invoiceStatement.setTimestamp(4, paidTimestamp);
                invoiceStatement.addBatch();
            }
            invoiceStatement.executeBatch();
        }
    }

    private static List<Integer> readInvoices(Connection connection) throws SQLException {
        List<Integer> ret = new ArrayList<>(NBR_INVOICES);
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT invoice_id FROM invoices")) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    final int invoiceId = resultSet.getInt(1);
                    ret.add(invoiceId);
                }
            }
        }
        return ret;
    }

    private static void createInvoiceDetails(Connection connection, List<Integer> invoiceIds, List<Product> products) throws SQLException {
        try (PreparedStatement invoiceDetailStatement = connection.prepareStatement("INSERT INTO invoice_details (invoice_id, product_id, list_price, sale_price, quantity) VALUES (?, ?, ?, ?, ?)")) {
            for (Integer invoiceId : invoiceIds) {
                final int nbrDetails = R.nextInt(5) + 1;
                for (int i = 0; i < nbrDetails; i++) {
                    final Product product = products.get(R.nextInt(products.size()));
                    invoiceDetailStatement.setInt(1, invoiceId);
                    invoiceDetailStatement.setInt(2, product.productId);
                    invoiceDetailStatement.setBigDecimal(3, product.price);
                    invoiceDetailStatement.setBigDecimal(4, product.price);
                    invoiceDetailStatement.setInt(5, R.nextInt(3) + 1);
                    invoiceDetailStatement.addBatch();
                }
            }
            invoiceDetailStatement.executeBatch();
        }
    }

    private static void createProducts(Connection connection) throws IOException, SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO products(part_no,description,price) VALUES (?, ?, ?)")) {
            final List<String[]> products = readFully("products.tsv");
            for (String[] product : products) {
                preparedStatement.setString(1, product[0]);
                preparedStatement.setString(2, product[1]);
                preparedStatement.setInt(3, R.nextInt(100) + 10);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
    }

    private static List<Product> readProducts(Connection connection) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT product_id, price FROM products")) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                final List<Product> ret = new ArrayList<>();
                while (resultSet.next()) {
                    final int productId = resultSet.getInt(1);
                    final BigDecimal price = resultSet.getBigDecimal(2);
                    ret.add(new Product(productId, null, null, price));
                }
                return ret;
            }
        }
    }

    private static void createCustomers(Connection connection) throws Exception {
        try (PreparedStatement customersStatement = connection.prepareStatement("INSERT INTO customers(name, address) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS)) {
            final List<String[]> customerPrefixes = readFully("customers.tsv");
            final List<String[]> products = readFully("products.tsv");
            final List<String[]> cities = readFully("cities.tsv");
            final List<String[]> streets = readFully("streets.tsv");
            for (String[] customerPrefix : customerPrefixes) {
                for (String[] product : products) {
                    final String name = customerPrefix[0] + product[0];
                    final String street = streets.get(R.nextInt(streets.size()))[0];
                    final String[] city = cities.get(R.nextInt(cities.size()));
                    final String address = String.format(
                            "%d %s, %s, %s %s",
                            R.nextInt(1000) + 1,
                            street,
                            city[0],
                            city[1],
                            city[2]
                    );
                    customersStatement.setString(1, name);
                    customersStatement.setString(2, address);
                    customersStatement.addBatch();
                }
            }
            customersStatement.executeBatch();
        }
    }

    private static List<Integer> readCustomers(Connection connection) throws Exception {
        final List<Integer> ids = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT customer_id FROM customers")) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    ids.add(resultSet.getInt(1));
                }
            }
        }
        return ids;
    }

    private static List<String[]> readFully(String filename) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(DataGenerator.class.getResourceAsStream(filename), StandardCharsets.UTF_8))) {
            final List<String[]> ret = new ArrayList<>();
            while (true) {
                final String line = reader.readLine();
                if (line == null) {
                    return ret;
                }
                final String[] parts = PATTERN.split(line);
                ret.add(parts);
            }
        }
    }
}
