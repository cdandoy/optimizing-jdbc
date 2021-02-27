package org.dandoy.fetchcustomers.v2;

import org.dandoy.fetchcustomers.ConnectionManager;
import org.dandoy.fetchcustomers.ElapsedStopWatch;
import org.dandoy.fetchcustomers.InvoicePrinter;
import org.dandoy.fetchcustomers.model.Invoice;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataExporter2 implements AutoCloseable {
    private static final int BATCH_SIZE = 100;
    private final List<Invoice> invoices = new ArrayList<>();
    private final CustomerReader customerReader;
    private final InvoiceDetailReader invoiceDetailReader;
    private final InvoicePrinter invoicePrinter;

    public static void main(String[] args) throws SQLException {
        try (Connection connection = ConnectionManager.createConnection()) {
            final ElapsedStopWatch stopWatch = new ElapsedStopWatch();
            try (DataExporter2 dataExporter = new DataExporter2(connection)) {
                dataExporter.run(connection);
            }
            System.out.println("Finished in " + stopWatch);
        }
    }

    public DataExporter2(Connection connection) throws SQLException {
        customerReader = new CustomerReader(connection);
        invoiceDetailReader = new InvoiceDetailReader(connection);
        invoicePrinter = InvoicePrinter.createInvoicePrinter();
    }

    private void run(Connection connection) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT invoice_id, customer_id, freight, created_date
                FROM invoices
                WHERE paid_date IS NULL
                """)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    final int invoiceId = resultSet.getInt("invoice_id");
                    final int customerId = resultSet.getInt("customer_id");
                    final BigDecimal freight = resultSet.getBigDecimal("freight");
                    final Timestamp createdDate = resultSet.getTimestamp("created_date");
                    addInvoice(
                            new Invoice(
                                    invoiceId,
                                    customerReader.getCustomer(customerId),
                                    freight,
                                    createdDate,
                                    null,
                                    invoiceDetailReader.getInvoiceDetails(invoiceId)
                            )
                    );
                }
            }
        }
    }

    private void addInvoice(Invoice invoice) throws SQLException {
        invoices.add(invoice);
        if (invoices.size() >= BATCH_SIZE) {
            flush();
        }
    }

    private void flush() throws SQLException {
        customerReader.flush();
        invoiceDetailReader.flush();
        invoices.forEach(invoicePrinter::printInvoice);
        invoices.clear();
    }

    @Override
    public void close() throws SQLException {
        flush();
        invoicePrinter.close();
    }
}
