/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */
package com.vertica.squeal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.PrintStream;

public class Script {

    enum Type { SQL, PIG };

    String name;
    Type type;
    List<String> stmts;
    boolean failureIsNotAnOption;

    public Script(String n, Type t, boolean canFail) {
	name = n;
	type = t;
	stmts = new ArrayList<String>();
	failureIsNotAnOption = canFail;
    }

    public void clear() {
	stmts.clear();
    }

    public void addStatement(String stmt) {
	stmts.add(stmt);
    }

    public String getName() { return name+"("+type+")"; }
    public Type getType() { return type; }
    public boolean isAllowedToFail() { return failureIsNotAnOption; }

    public List<String> getStatements() {
	return stmts;
    }

    public void print(PrintStream out) {
	out.println("Script: "+getName()+(failureIsNotAnOption ? "[ErrorsOK]" : ""));
	for (String s : stmts) {
	    out.println(" "+s);
	}
    }
}