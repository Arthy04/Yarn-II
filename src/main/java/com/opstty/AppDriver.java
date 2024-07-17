package com.opstty;

import com.opstty.job.WordCount;
import com.opstty.job.DistrictsContainingTrees;
import com.opstty.job.TreeSpeciesList;
import com.opstty.job.TreeKindCount;
import com.opstty.job.TreeMaxHeight;
import com.opstty.job.TreeHeightSortJob;
import com.opstty.job.OldestTreeJob;
import com.opstty.job.DistrictMostTreesJob;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtsContainingTrees", DistrictsContainingTrees.class,
                    "A map/reduce program that lists districts containing trees.");
            programDriver.addClass("treeSpeciesList", TreeSpeciesList.class,
                    "A map/reduce program that lists the different species of trees.");
            programDriver.addClass("treeKindCount", TreeKindCount.class,
                    "A map/reduce program that counts the number of trees by kinds.");
            programDriver.addClass("treeMaxHeight", TreeMaxHeight.class,
                    "A map/reduce program that finds the maximum height of trees for each kind.");
            programDriver.addClass("treeHeightSort", TreeHeightSortJob.class,
                    "A map/reduce program that sorts the tree heights from smallest to largest.");
            programDriver.addClass("oldestTree", OldestTreeJob.class,
                    "A map/reduce program that finds the district containing the oldest tree.");
            programDriver.addClass("districtMostTrees", DistrictMostTreesJob.class,
                    "A map/reduce program that finds the district containing the most trees.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
