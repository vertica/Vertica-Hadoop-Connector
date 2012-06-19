package com.vertica.squeal;

import java.util.*;

public class NameUtil {

    /**
     * converts string to only alpha-numeric characters & no spaces.
     * Stick in an _ for each group of non alpha-numeric characters
     * (excepting any leading such chars).
     */
    public static String sqlTableNameify(String name) {
	int l = name.length();
	// empty string?
	if (l == 0) return "blank";
	StringBuilder sb = new StringBuilder();
	// starts with number?
	if (Character.isDigit(name.charAt(0))) sb.append('_');
	boolean seenOdd = false;
	for (int i=0; i < l; i++) {
	    char c = name.charAt(i);
	    if (Character.isLetterOrDigit(c)) {
		sb.append(c);
		seenOdd = false;
	    } else if (!seenOdd && sb.length() > 0) {
		sb.append('_');
		seenOdd = true;
	    }
	}
	return sb.toString();
    }

    public static String uniquify(Set<String> existing, String n) {
	if (existing.contains(n)) {
	    StringBuilder n2 = new StringBuilder(n);
	    n2.append('1');
	    int i=1, l = n.length();
	    while (existing.contains(n2)) {
		i++;
		n2.setLength(l);
		n2.append(String.valueOf(i));
	    }
	    return n2.toString();
	} else {
	    return n;
	}
    }

    public static String unURL(String s) {
	int i = s.indexOf("://");
	if (i > 0) {
	    return s.substring(i+3,s.length());
	} else {
	    return s;
	}
    }
}