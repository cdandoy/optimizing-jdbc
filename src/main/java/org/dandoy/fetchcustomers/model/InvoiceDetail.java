package org.dandoy.fetchcustomers.model;

import java.math.BigDecimal;

public class InvoiceDetail {
    public final int invoiceDetailId;
    public final Product product;
    public final BigDecimal listPrice;
    public final BigDecimal salePrice;
    public final int quantity;

    public InvoiceDetail(int invoiceDetailId, Product product, BigDecimal listPrice, BigDecimal salePrice, int quantity) {
        this.invoiceDetailId = invoiceDetailId;
        this.product = product;
        this.listPrice = listPrice;
        this.salePrice = salePrice;
        this.quantity = quantity;
    }
}
