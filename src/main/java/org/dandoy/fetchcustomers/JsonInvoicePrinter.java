package org.dandoy.fetchcustomers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import org.dandoy.fetchcustomers.model.Invoice;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class JsonInvoicePrinter extends InvoicePrinter {
    private final JsonGenerator jsonGenerator;

    protected JsonInvoicePrinter() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
        final File file = new File("invoices.json");
        try {
            jsonGenerator = mapper.getFactory().createGenerator(
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    new FileOutputStream(file),
                                    StandardCharsets.UTF_8
                            )
                    )
            );
            jsonGenerator.writeStartArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open " + file.getAbsolutePath(), e);
        }
    }

    @Override
    public void printInvoice(Invoice invoice) {
        try {
            jsonGenerator.writeObject(invoice);
            if (invoice.invoiceDetails.isEmpty()) throw new IllegalStateException("Missing invoice detail");
            if (invoice.customer.name == null) throw new IllegalStateException("Missing customer");
            if (invoice.invoiceDetails.stream().anyMatch(it -> it.product.partNo == null)) throw new IllegalStateException("Missing product");
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write the invoice " + invoice.invoiceId, e);
        }
    }

    @Override
    public void close() {
        try {
            jsonGenerator.writeEndArray();
            jsonGenerator.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close the output", e);
        }
    }
}
