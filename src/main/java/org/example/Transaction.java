package org.example;

import org.apache.avro.generic.GenericRecord;
import org.json.JSONObject;

public class Transaction {
    public String user_id;
    public long transaction_timestamp_millis;
    public double amount;
    public String currency;
    public int counterpart_id;

    public Transaction() {}

    public Transaction(String jsonString) {
        JSONObject json = new JSONObject(jsonString);
        user_id = (new Integer(json.getInt("user_id"))).toString();
        transaction_timestamp_millis = json.getLong("transaction_timestamp_millis");
        amount = json.getDouble("amount");
        currency = json.getString("currency");
        counterpart_id = json.getInt("counterpart_id");
    }
    
    public Transaction(GenericRecord t){
        user_id = t.get("user_id").toString();
        amount = (double) t.get("amount");
        transaction_timestamp_millis = (long) t.get("transaction_timestamp_millis");
        amount = (double) t.get("amount");
        currency = t.get("currency").toString();
        counterpart_id = (int) t.get("counterpart_id");
    }

    @Override
    public String toString() {
        return new JSONObject(this).toString();
    }
}