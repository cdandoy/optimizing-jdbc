package org.dandoy.fetchcustomers.v2;


import org.apache.commons.lang3.StringUtils;
import org.dandoy.fetchcustomers.model.InvoiceDetail;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InvoiceDetailReader implements AutoCloseable {
    private static final int BATCH_SIZE = 100;
    private final PreparedStatement preparedStatement;
    private final Map<Integer, List<InvoiceDetail>> pending = new HashMap<>();
    private final ProductReader productReader;

    public InvoiceDetailReader(Connection connection) throws SQLException {
        final String sql = "SELECT invoice_detail_id, invoice_id, product_id, list_price, sale_price, quantity\n" +
                           "FROM invoice_details\n" +
                           "WHERE invoice_id IN (" + StringUtils.repeat("?", ",", BATCH_SIZE) + ")\n";
        preparedStatement = connection.prepareStatement(sql);
        productReader = new ProductReader(connection);
    }

    List<InvoiceDetail> getInvoiceDetails(int invoiceId) throws SQLException {
        final List<InvoiceDetail> invoiceDetails = pending.computeIfAbsent(invoiceId, it -> new ArrayList<>());
        if (pending.size() == BATCH_SIZE) {
            flush();
        }
        return invoiceDetails;
    }

    public void flush() throws SQLException {
        int i = 0;
        for (Map.Entry<Integer, List<InvoiceDetail>> entry : pending.entrySet()) {
            preparedStatement.setInt(++i, entry.getKey());
        }
        while (i < BATCH_SIZE) {
            preparedStatement.setNull(++i, Types.INTEGER);
        }
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                final int invoiceDetailId = resultSet.getInt("invoice_detail_id");
                final int invoiceId = resultSet.getInt("invoice_id");
                final int productId = resultSet.getInt("product_id");
                final BigDecimal listPrice = resultSet.getBigDecimal("list_price");
                final BigDecimal salePrice = resultSet.getBigDecimal("sale_price");
                final int quantity = resultSet.getInt("quantity");
                final InvoiceDetail invoiceDetail = new InvoiceDetail(
                        invoiceDetailId,
                        productReader.getProduct(productId),
                        listPrice,
                        salePrice,
                        quantity);
                pending.get(invoiceId).add(invoiceDetail);
            }
        }
        productReader.flush();
        pending.clear();
    }

    @Override
    public void close() throws SQLException {
        flush();
        preparedStatement.close();
        productReader.close();
    }
}
