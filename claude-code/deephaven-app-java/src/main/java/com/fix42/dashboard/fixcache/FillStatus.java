package com.fix42.dashboard.fixcache;

import java.util.List;

/** Values of the executions {@code FillStatus} column (doc 01 section 6). */
public final class FillStatus {

    public static final String NORMAL = "NORMAL";
    public static final String BUSTED = "BUSTED";
    public static final String CORRECTED = "CORRECTED";
    public static final String DK = "DK";

    public static final List<String> ALL = List.of(NORMAL, BUSTED, CORRECTED, DK);

    private FillStatus() {}
}
