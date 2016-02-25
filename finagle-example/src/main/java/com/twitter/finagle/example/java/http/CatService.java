package com.twitter.finagle.example.java.http;

public final class CatService {
    static CatsDB db = new CatsDB();

    public static Cat find(int id) {
        return db.get(id);
    }
}
