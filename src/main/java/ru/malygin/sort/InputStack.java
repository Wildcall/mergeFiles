package ru.malygin.sort;

import java.io.BufferedReader;
import java.io.IOException;

public class InputStack {

    private final BufferedReader bufferedReader;
    private String cache;

    public InputStack(BufferedReader br) throws IOException {
        this.bufferedReader = br;
        getLine();
    }

    public String pop() throws IOException {
        String result = peek();
        getLine();
        return result;
    }

    public void close() throws IOException {
        this.bufferedReader.close();
    }

    public boolean empty() {
        return cache == null;
    }

    public String peek(){
        return this.cache;
    }

    private void getLine() throws IOException {
        this.cache = bufferedReader.readLine();
    }
}
