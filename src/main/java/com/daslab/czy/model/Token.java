package com.daslab.czy.model;

public class Token {
    public String word;
    public String pos;
    public int start;
    public int end;

    public Token(String word, String pos, int start, int end){
        this.word = word;
        this.pos = pos;
        this.start = start;
        this.end = end;
    }

    public Token(String word, String pos){
        this.word = word;
        this.pos = pos;
        this.start = 0;
        this.end = 0;
    }

    @Override
    public String toString() {
        return this.word + "/" + this.pos + " [" + this.start + ":" + this.end + "]";
    }
}
