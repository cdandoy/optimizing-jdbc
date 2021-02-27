package org.dandoy.fetchcustomers.model;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

public class Invoice {
    public final int invoiceId;
    public final Customer customer;
    public final BigDecimal freight;
    public final Timestamp createdDate;
    public final Timestamp paidDate;
    public final List<InvoiceDetail> invoiceDetails;

    public Invoice(int invoiceId) {
        this(invoiceId, null, null, null, null, null);
    }

    public Invoice(int invoiceId, Customer customer, BigDecimal freight, Timestamp createdDate, Timestamp paidDate, List<InvoiceDetail> invoiceDetails) {
        this.invoiceId = invoiceId;
        this.customer = customer;
        this.freight = freight;
        this.createdDate = createdDate;
        this.paidDate = paidDate;
        this.invoiceDetails = invoiceDetails;
    }
}
