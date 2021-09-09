package ru.malygin.sort;

public class StringSizeCalculator {
    private static final int OBJ_HEADER;
    private static final int ARR_HEADER;
    private static final int INT_FIELDS = 12;
    private static final int OBJ_REF;
    private static final int OBJ_OVERHEAD;

    private StringSizeCalculator() {
    }

    static {
        boolean IS_64_BIT_JVM = true;
        String arch = System.getProperty("sun.arch.data.model");
        if (arch != null) {
            if (arch.contains("32")) {
                IS_64_BIT_JVM = false;
            }
        }
        OBJ_HEADER = IS_64_BIT_JVM ? 16 : 8;
        ARR_HEADER = IS_64_BIT_JVM ? 24 : 12;
        OBJ_REF = IS_64_BIT_JVM ? 8 : 4;
        OBJ_OVERHEAD = OBJ_HEADER + INT_FIELDS + OBJ_REF + ARR_HEADER;
    }

    public static long estimatedSizeOf(String s) {
        return (s.length() * 2L) + OBJ_OVERHEAD;
    }
}
