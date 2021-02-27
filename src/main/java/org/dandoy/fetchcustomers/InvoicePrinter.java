package org.dandoy.fetchcustomers;

import org.dandoy.fetchcustomers.model.Invoice;

public abstract class InvoicePrinter implements AutoCloseable {
    public static InvoicePrinter NOOP = new InvoicePrinter() {
        @Override
        public void printInvoice(Invoice invoice) {
        }

        @Override
        public void close() {
        }
    };

    public static InvoicePrinter createInvoicePrinter() {
//        return new JsonInvoicePrinter();
        return NOOP;
    }

    public abstract void printInvoice(Invoice invoice);

    @Override
    public abstract void close();
}
