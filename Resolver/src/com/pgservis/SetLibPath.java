package com.pgservis;

import java.lang.reflect.Field;

public class SetLibPath {

    public static void appendToPath(String dir){

        String path = System.getProperty("java.library.path");
        path = dir + ";" + path;
        System.setProperty("java.library.path", path);

        try {
            final Field sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
            sysPathsField.setAccessible(true);
            sysPathsField.set(null, null);
        }
        catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

}
