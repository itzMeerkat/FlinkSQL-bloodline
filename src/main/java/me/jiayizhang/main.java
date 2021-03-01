package me.jiayizhang;


import me.jiayizhang.graghgen.GraphGenerator;
import me.jiayizhang.sqlparser.TaskMetaData;
import me.jiayizhang.sqlparser.TaskParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;


public class main {
    public static GraphGenerator generator = new GraphGenerator();
    static void ReadAndParse(Path path) {
//        System.out.println(path.toString());
        try {
            StringBuilder content = new StringBuilder();
            try {
                File myObj = new File(path.toString());
                Scanner myReader = new Scanner(myObj);
                while (myReader.hasNextLine()) {
                    String data = myReader
                            .nextLine()
                            .replaceAll("--.*","");
                    content.append(data);
                    content.append("\n");
                }
                myReader.close();
            } catch (FileNotFoundException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }


            String taskName = path.getFileName().toString();
            System.out.printf("****************** %s ******************\n", taskName);
            TaskMetaData task = TaskParser.ParseTask(taskName, content.toString());

            task.GetSources().forEach((e)-> generator.AddSourceRelation(taskName, e.MakeKey()));
            task.GetSinks().forEach((e)-> generator.AddSinkRelation(taskName, e.MakeKey()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) {
        String path = "";
        try {
            Files.walk(Paths.get(path))
                    .filter((n) -> n.toString().endsWith(".sql"))
                    .forEach(main::ReadAndParse);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String res = generator.GenerateGraph();
        System.out.println(res);
    }
}