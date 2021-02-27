package org.dandoy.fetchcustomers.v2;


import org.apache.commons.lang3.StringUtils;
import org.dandoy.fetchcustomers.model.Product;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class ProductReader implements AutoCloseable {
    private static final int BATCH_SIZE = 100;
    private final PreparedStatement preparedStatement;
    private final Map<Integer, Product> pending = new HashMap<>();

    public ProductReader(Connection connection) throws SQLException {
        final String sql = "SELECT product_id, part_no, description, price\n" +
                           "FROM products\n" +
                           "WHERE product_id IN (" + StringUtils.repeat("?", ",", BATCH_SIZE) + ")\n";
        preparedStatement = connection.prepareStatement(sql);
    }

    Product getProduct(int productId) throws SQLException {
        final Product product = pending.computeIfAbsent(productId, Product::new);
        if (pending.size() == BATCH_SIZE) {
            flush();
        }
        return product;
    }

    public void flush() throws SQLException {
        int i = 0;
        for (Map.Entry<Integer, Product> entry : pending.entrySet()) {
            preparedStatement.setInt(++i, entry.getKey());
        }
        while (i < BATCH_SIZE) {
            preparedStatement.setNull(++i, Types.INTEGER);
        }
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                final int productId = resultSet.getInt("product_id");
                final String partNo = resultSet.getString("part_no");
                final String description = resultSet.getString("description");
                final BigDecimal price = resultSet.getBigDecimal("price");
                final Product product = pending.get(productId);
                product.setPartNo(partNo)
                        .setDescription(description)
                        .setPrice(price);
            }
        }
        pending.clear();
    }

    @Override
    public void close() throws SQLException {
        flush();
    }
}
