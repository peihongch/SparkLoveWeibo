package util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
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

    public static List<String> getIds() throws FileNotFoundException {
        List<String> ids = new ArrayList<String>();
        FileInputStream in = new FileInputStream("src/main/resources/university_list.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        reader.lines().forEach(line -> {
            String[] splits = line.split(" ");
            ids.add(splits[0]);
        });
        return ids;
    }

    public static void main(String[] args) throws FileNotFoundException {
        List<String> re = getIds();
        for (String r:re) {
            System.out.println(r);
        }
    }
}
