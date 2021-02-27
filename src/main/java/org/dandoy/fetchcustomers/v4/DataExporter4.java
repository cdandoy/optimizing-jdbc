package org.dandoy.fetchcustomers.v4;

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

public class DataExporter4 {

    public static void main(String[] args) throws SQLException {
        try (Connection connection = ConnectionManager.createConnection()) {
            DataExporter4 dataExporter = new DataExporter4();
            try (InvoicePrinter invoicePrinter = InvoicePrinter.createInvoicePrinter()) {
                final ElapsedStopWatch stopWatch = new ElapsedStopWatch();
                dataExporter.run(connection, invoicePrinter);
                System.out.println("Finished in " + stopWatch);
            }
        }
    }

    private void run(Connection connection, InvoicePrinter invoicePrinter) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT i.invoice_id,
                       i.freight,
                       i.created_date,
                       id.invoice_detail_id,
                       id.list_price,
                       id.sale_price,
                       id.quantity,
                       c.customer_id,
                       c.name,
                       c.address,
                       p.product_id,
                       p.part_no,
                       p.description,
                       p.price
                FROM invoices i
                         JOIN invoice_details id ON i.invoice_id = id.invoice_id
                         JOIN customers c ON i.customer_id = c.customer_id
                         JOIN products p ON id.product_id = p.product_id
                WHERE i.paid_date IS NULL
                ORDER BY i.invoice_id
                """)) {
            // Only remember the last invoice. Start with a fake/sentinel
            Invoice lastInvoice = new Invoice(-1);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    final int invoiceId = resultSet.getInt("invoice_id");
                    if (invoiceId != lastInvoice.invoiceId) {   // new invoice?
                        if (lastInvoice.invoiceId >= 0) {       // Don't write the fake invoice
                            invoicePrinter.printInvoice(lastInvoice);
                        }
                        final int customerId = resultSet.getInt("customer_id");
                        final String name = resultSet.getString("name");
                        final String address = resultSet.getString("address");
                        final BigDecimal freight = resultSet.getBigDecimal("freight");
                        final Timestamp createdDate = resultSet.getTimestamp("created_date");
                        lastInvoice = new Invoice(
                                invoiceId,
                                new Customer(
                                        customerId,
                                        name,
                                        address
                                ),
                                freight,
                                createdDate,
                                null,
                                new ArrayList<>()
                        );
                    }

                    final int productId = resultSet.getInt("product_id");
                    final String partNo = resultSet.getString("part_no");
                    final String description = resultSet.getString("description");
                    final BigDecimal price = resultSet.getBigDecimal("price");
                    final Product product = new Product(
                            productId,
                            partNo,
                            description,
                            price
                    );

                    final int invoiceDetailId = resultSet.getInt("invoice_detail_id");
                    final BigDecimal listPrice = resultSet.getBigDecimal("list_price");
                    final BigDecimal salePrice = resultSet.getBigDecimal("sale_price");
                    final int quantity = resultSet.getInt("quantity");
                    final InvoiceDetail invoiceDetail = new InvoiceDetail(
                            invoiceDetailId,
                            product,
                            listPrice,
                            salePrice,
                            quantity
                    );
                    lastInvoice.invoiceDetails.add(invoiceDetail);
                }
            }
            if (lastInvoice.invoiceId >= 0) {
                invoicePrinter.printInvoice(lastInvoice);
            }
        }
    }
}
