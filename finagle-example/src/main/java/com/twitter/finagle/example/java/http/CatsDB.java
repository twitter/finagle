package com.twitter.finagle.example.java.http;

import java.util.HashMap;

public class CatsDB {
    private final static HashMap<Integer, Cat> db = new HashMap<>();

    public Cat get(Integer id) {
        if (db.isEmpty())
            addExampleCats();

        return db.get(id);
    }

    private void addExampleCats() {
        db.put(0, new Cat("Doug"));
        db.put(1, new Cat("Ozzy"));
        db.put(2, new Cat("Logan"));
        db.put(3, new Cat("Dylan"));
    }
}
