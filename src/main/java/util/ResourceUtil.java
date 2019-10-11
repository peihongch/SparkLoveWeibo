package util;

import java.io.*;
import java.util.Properties;

public final class ResourceUtil {
    public static Properties getUniversityList() throws FileNotFoundException {
        Properties props = new Properties();
        FileInputStream in = new FileInputStream("src/main/resources/university_list.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        reader.lines().forEach(line -> {
            String[] splits = line.split(" ");
            props.put(splits[0], splits[1]);
        });
        return props;
    }

    public static Properties getIdNamePairs() throws FileNotFoundException {
        return getUniversityList();
    }

    public static Properties getNameIdPairs() throws FileNotFoundException {
        Properties props = new Properties();
        FileInputStream in = new FileInputStream("src/main/resources/university_list.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        reader.lines().forEach(line -> {
            String[] splits = line.split(" ");
            props.put(splits[1], splits[0]);
        });
        return props;
    }

    public static void main(String[] args) throws FileNotFoundException {
        Properties props = getUniversityList();
        for (Object key : props.keySet()) {
            System.out.println(key + "\t" + props.getProperty((String) key));
        }
    }
}
