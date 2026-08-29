package com.fix42.dashboard.fixcache;

import java.util.List;

/** Values of the {@code PendingAction} column (doc 01 section 4). */
public final class PendingAction {

    public static final String NONE = "NONE";
    public static final String NEW = "NEW";
    public static final String CANCEL = "CANCEL";
    public static final String REPLACE = "REPLACE";

    public static final List<String> ALL = List.of(NONE, NEW, CANCEL, REPLACE);

    private PendingAction() {}
}
