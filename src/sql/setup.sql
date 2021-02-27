DROP TABLE IF EXISTS invoice_details;
DROP TABLE IF EXISTS invoices;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers
(
    customer_id INTEGER IDENTITY PRIMARY KEY,
    name        VARCHAR(255),
    address     VARCHAR(255)
);
GO
CREATE TABLE products
(
    product_id  INTEGER IDENTITY PRIMARY KEY,
    part_no     VARCHAR(255) NOT NULL,
    description VARCHAR(255) NOT NULL,
    price       MONEY,
)
CREATE TABLE invoices
(
    invoice_id   INTEGER IDENTITY PRIMARY KEY,
    customer_id  INTEGER
        CONSTRAINT invoices_customers_fk REFERENCES customers,
    freight      MONEY,
    created_date DATETIME,
    paid_date    DATETIME
)
GO
CREATE TABLE invoice_details
(
    invoice_detail_id INTEGER IDENTITY PRIMARY KEY,
    invoice_id        INTEGER NOT NULL
        CONSTRAINT invoice_details_invoices_fk REFERENCES invoices,
    product_id        INTEGER NOT NULL
        CONSTRAINT invoice_details_products_fk REFERENCES products,
    list_price        MONEY   NOT NULL,
    sale_price        MONEY   NOT NULL,
    quantity          INTEGER NOT NULL,
)
GO
CREATE INDEX invoices_paid_date_index
    ON invoices (paid_date)
GO
