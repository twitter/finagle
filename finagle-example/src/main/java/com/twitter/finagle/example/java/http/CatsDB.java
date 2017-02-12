package com.twitter.finagle.example.java.http;

import java.util.HashMap;

public class CatsDB {
    private static final HashMap<Integer, Cat> DB = new HashMap<>();

    /**
     * Get the Cat for a given id.
     */
    public Cat get(Integer id) {
        if (DB.isEmpty()) {
            addExampleCats();
        }

        return DB.get(id);
    }

    private void addExampleCats() {
        DB.put(0, new Cat("Doug"));
        DB.put(1, new Cat("Ozzy"));
        DB.put(2, new Cat("Logan"));
        DB.put(3, new Cat("Dylan"));
    }
}
