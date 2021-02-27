package org.dandoy.fetchcustomers.model;

public class Customer {
    public final int customerId;
    public String name;
    public String address;

    public Customer(int customerId) {
        this(customerId, null, null);
    }

    public Customer(int customerId, String name, String address) {
        this.customerId = customerId;
        this.name = name;
        this.address = address;
    }

    public Customer setName(String name) {
        this.name = name;
        return this;
    }

    public Customer setAddress(String address) {
        this.address = address;
        return this;
    }
}
