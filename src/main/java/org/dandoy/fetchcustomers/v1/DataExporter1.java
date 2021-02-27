package org.dandoy.fetchcustomers.v1;

import org.dandoy.fetchcustomers.ConnectionManager;
import org.dandoy.fetchcustomers.ElapsedStopWatch;
import org.dandoy.fetchcustomers.InvoicePrinter;
import org.dandoy.fetchcustomers.model.Customer;
import org.dandoy.fetchcustomers.model.Invoice;
import org.dandoy.fetchcustomers.model.InvoiceDetail;
import org.dandoy.fetchcustomers.model.Product;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataExporter1 implements AutoCloseable {
    private final PreparedStatement customerStatement;
    private final PreparedStatement invoiceDetailStatement;
    private final PreparedStatement productStatement;

    public DataExporter1(Connection connection) throws SQLException {
        customerStatement = connection.prepareStatement("""
                SELECT name, address
                FROM customers
                WHERE customer_id = ?
                """);
        invoiceDetailStatement = connection.prepareStatement("""
                SELECT invoice_detail_id, product_id, list_price, sale_price, quantity
                FROM invoice_details
                WHERE invoice_id = ?
                """);
        productStatement = connection.prepareStatement("""
                SELECT part_no, description, price
                FROM products
                WHERE product_id = ?
                """);
    }

    @Override
    public void close() throws SQLException {
        productStatement.close();
        invoiceDetailStatement.close();
        customerStatement.close();
    }

    public static void main(String[] args) throws SQLException {
        try (Connection connection = ConnectionManager.createConnection()) {
            try (DataExporter1 dataExporter = new DataExporter1(connection)) {
                try (InvoicePrinter invoicePrinter = InvoicePrinter.createInvoicePrinter()) {
                    final ElapsedStopWatch stopWatch = new ElapsedStopWatch();
                    dataExporter.run(connection, invoicePrinter);
                    System.out.println("Finished in " + stopWatch);
                }
            }
        }
    }

    private void run(Connection connection, InvoicePrinter invoicePrinter) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT invoice_id, customer_id, freight, created_date
                FROM invoices
                WHERE paid_date IS NULL
                """)) {
            // For each invoice
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    final int invoiceId = resultSet.getInt("invoice_id");
                    final int customerId = resultSet.getInt("customer_id");
                    final BigDecimal freight = resultSet.getBigDecimal("freight");
                    final Timestamp createdDate = resultSet.getTimestamp("created_date");
                    final Invoice invoice = new Invoice(
                            invoiceId,
                            getCustomer(customerId),    // read the customer
                            freight,
                            createdDate,
                            null,
                            getInvoiceDetails(invoiceId) // read the invoice details
                    );
                    invoicePrinter.printInvoice(invoice);
                }
            }
        }
    }

    /**
     * @return the invoice details for the invoiceId
     */
    private List<InvoiceDetail> getInvoiceDetails(int invoiceId) throws SQLException {
        final List<InvoiceDetail> ret = new ArrayList<>();
        invoiceDetailStatement.setInt(1, invoiceId);
        try (ResultSet resultSet = invoiceDetailStatement.executeQuery()) {
            while (resultSet.next()) {
                final int invoiceDetailId = resultSet.getInt("invoice_detail_id");
                final int productId = resultSet.getInt("product_id");
                final BigDecimal listPrice = resultSet.getBigDecimal("list_price");
                final BigDecimal salePrice = resultSet.getBigDecimal("sale_price");
                final int quantity = resultSet.getInt("quantity");
                final InvoiceDetail invoiceDetail = new InvoiceDetail(
                        invoiceDetailId,
                        getProduct(productId),
                        listPrice,
                        salePrice,
                        quantity
                );
                ret.add(invoiceDetail);
            }
        }
        return ret;
    }

    /**
     * @return the Product for the productId
     */
    private Product getProduct(int productId) throws SQLException {
        productStatement.setInt(1, productId);
        try (ResultSet resultSet = productStatement.executeQuery()) {
            if (!resultSet.next()) throw new IllegalStateException("Product not found: " + productId);
            final String partNo = resultSet.getString("part_no");
            final String description = resultSet.getString("description");
            final BigDecimal price = resultSet.getBigDecimal("price");
            return new Product(productId, partNo, description, price);
        }
    }

    /**
     * @return the Customer for the customerId
     */
    private Customer getCustomer(int customerId) throws SQLException {
        customerStatement.setInt(1, customerId);
        try (ResultSet resultSet = customerStatement.executeQuery()) {
            if (!resultSet.next()) throw new IllegalStateException("Customer not found: " + customerId);
            final String name = resultSet.getString("name");
            final String address = resultSet.getString("address");
            return new Customer(customerId, name, address);
        }
    }
}
