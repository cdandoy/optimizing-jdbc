package org.dandoy.fetchcustomers.v2;


import org.apache.commons.lang3.StringUtils;
import org.dandoy.fetchcustomers.model.Customer;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class CustomerReader implements AutoCloseable {
    private static final int BATCH_SIZE = 100;
    private final PreparedStatement preparedStatement;
    private final Map<Integer, Customer> pending = new HashMap<>();

    public CustomerReader(Connection connection) throws SQLException {
        final String sql = "SELECT customer_id, name, address\n" +
                           "FROM customers\n" +
                           "WHERE customer_id IN (" + StringUtils.repeat("?", ",", BATCH_SIZE) + ")\n";
        preparedStatement = connection.prepareStatement(sql);
    }

    Customer getCustomer(int customerId) throws SQLException {
        final Customer customer = pending.computeIfAbsent(customerId, Customer::new);
        if (pending.size() == BATCH_SIZE) {
            flush();
        }
        return customer;
    }

    public void flush() throws SQLException {
        int i = 0;
        for (Map.Entry<Integer, Customer> entry : pending.entrySet()) {
            preparedStatement.setInt(++i, entry.getKey());
        }
        while (i < BATCH_SIZE) {
            preparedStatement.setNull(++i, Types.INTEGER);
        }
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                final int customerId = resultSet.getInt("customer_id");
                final String name = resultSet.getString("name");
                final String address = resultSet.getString("address");
                final Customer customer = pending.get(customerId);
                customer.setName(name)
                        .setAddress(address);
            }
        }
        pending.clear();
    }

    @Override
    public void close() throws SQLException {
        flush();
    }
}
