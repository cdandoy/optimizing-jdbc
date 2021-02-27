package org.dandoy.fetchcustomers.v3;

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
import java.util.HashMap;
import java.util.Map;

public class DataExporter3 {
    public static void main(String[] args) throws SQLException {
        try (Connection connection = ConnectionManager.createConnection()) {
            DataExporter3 dataExporter = new DataExporter3();
            final ElapsedStopWatch stopWatch = new ElapsedStopWatch();
            dataExporter.run(connection);
            System.out.println("Finished in " + stopWatch);
        }
    }

    private void run(Connection connection) throws SQLException {
        // Execute a multi-statement.
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                -- Read the invoices into a temp table
                SELECT invoice_id, customer_id, freight, created_date
                INTO #invoices
                FROM invoices
                WHERE paid_date IS NULL
                                
                -- output the invoice temp table
                SELECT invoice_id, customer_id, freight, created_date
                FROM #invoices
                                
                -- Read the invoice details into a temp table
                SELECT id.invoice_detail_id, id.invoice_id, id.product_id, id.list_price, id.sale_price, id.quantity
                INTO #invoice_details
                FROM #invoices i JOIN invoice_details id ON i.invoice_id = id.invoice_id;
                                
                -- output the invoice details temp table
                SELECT invoice_detail_id, invoice_id, product_id, list_price, sale_price, quantity
                FROM #invoice_details;
                                
                -- select the distinct customers
                WITH x AS (
                    SELECT DISTINCT customer_id
                    FROM #invoices
                )
                SELECT c.customer_id, name, address
                FROM x
                         JOIN customers c ON c.customer_id = x.customer_id;
                                
                -- select the distinct products
                WITH x AS (
                    SELECT DISTINCT product_id
                    FROM #invoice_details
                )
                SELECT p.product_id, part_no, description, price
                FROM x
                         JOIN products p ON x.product_id = p.product_id;
                """)) {
            final Map<Integer, Invoice> invoiceMap = new HashMap<>();   // Map of invoiceId  -> Invoice
            final Map<Integer, Customer> customerMap = new HashMap<>(); // Map of customerId -> Customer
            final Map<Integer, Product> productMap = new HashMap<>();   // Map of productId  -> Product
            try (ResultSet invoiceResultSet = preparedStatement.executeQuery()) {
                //
                // Read the invoices
                //
                while (invoiceResultSet.next()) {
                    final int invoiceId = invoiceResultSet.getInt("invoice_id");
                    final int customerId = invoiceResultSet.getInt("customer_id");
                    final BigDecimal freight = invoiceResultSet.getBigDecimal("freight");
                    final Timestamp createdDate = invoiceResultSet.getTimestamp("created_date");
                    final Customer customer = customerMap.computeIfAbsent(customerId, Customer::new);
                    invoiceMap.put(
                            invoiceId,
                            new Invoice(
                                    invoiceId,
                                    customer,
                                    freight,
                                    createdDate,
                                    null,
                                    new ArrayList<>()
                            )
                    );
                }
                //
                // Read the invoice details
                //
                preparedStatement.getMoreResults();
                try (ResultSet invoiceDetailResultSet = preparedStatement.getResultSet()) {
                    while (invoiceDetailResultSet.next()) {
                        final int invoiceDetailId = invoiceDetailResultSet.getInt("invoice_detail_id");
                        final int invoiceId = invoiceDetailResultSet.getInt("invoice_id");
                        final int productId = invoiceDetailResultSet.getInt("product_id");
                        final BigDecimal listPrice = invoiceDetailResultSet.getBigDecimal("list_price");
                        final BigDecimal salePrice = invoiceDetailResultSet.getBigDecimal("sale_price");
                        final int quantity = invoiceDetailResultSet.getInt("quantity");
                        final Product product = productMap.computeIfAbsent(productId, Product::new);
                        // Add the details to the corresponding invoice
                        final Invoice invoice = invoiceMap.get(invoiceId);
                        invoice.invoiceDetails.add(
                                new InvoiceDetail(
                                        invoiceDetailId,
                                        product,
                                        listPrice,
                                        salePrice,
                                        quantity
                                )
                        );
                    }
                }
                //
                // Read the customers
                //
                preparedStatement.getMoreResults();
                try (ResultSet customerResultSet = preparedStatement.getResultSet()) {
                    while (customerResultSet.next()) {
                        final int customerId = customerResultSet.getInt("customer_id");
                        final String name = customerResultSet.getString("name");
                        final String address = customerResultSet.getString("address");
                        final Customer customer = customerMap.get(customerId);
                        customer.setName(name)
                                .setAddress(address);
                    }
                }
                //
                // Read the products
                //
                preparedStatement.getMoreResults();
                try (ResultSet productResultSet = preparedStatement.getResultSet()) {
                    while (productResultSet.next()) {
                        final int productId = productResultSet.getInt("product_id");
                        final String partNo = productResultSet.getString("part_no");
                        final String description = productResultSet.getString("description");
                        final BigDecimal price = productResultSet.getBigDecimal("price");
                        final Product product = productMap.get(productId);
                        product.setPartNo(partNo)
                                .setDescription(description)
                                .setPrice(price);
                    }
                }
            }
            try (InvoicePrinter invoicePrinter = InvoicePrinter.createInvoicePrinter()) {
                for (Invoice invoice : invoiceMap.values()) {
                    invoicePrinter.printInvoice(invoice);
                }
            }
        }
    }
}
