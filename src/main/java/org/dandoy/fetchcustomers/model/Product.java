package org.dandoy.fetchcustomers.model;

import java.math.BigDecimal;

public class Product {
    public final int productId;
    public String partNo;
    public String description;
    public BigDecimal price;

    public Product(int productId) {
        this(productId, null, null, null);
    }

    public Product(int productId, String partNo, String description, BigDecimal price) {
        this.productId = productId;
        this.partNo = partNo;
        this.description = description;
        this.price = price;
    }

    public Product setPartNo(String partNo) {
        this.partNo = partNo;
        return this;
    }

    public Product setDescription(String description) {
        this.description = description;
        return this;
    }

    public Product setPrice(BigDecimal price) {
        this.price = price;
        return this;
    }
}
