/*******************************************************************************
 * Copyright © 2020 EMBL - European Bioinformatics Institute
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License.
 ******************************************************************************/

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * Batch Query Indexer
 * The list elements should be:
 * [1]: fully-qualified csv input directory path
 */
public class Main {
    String       inputPath;
    SparkSession spark;

    public static void main(String[] args) {
        Main main = new Main();
        main.initialise(args);
        main.index();
    }

    public void initialise(String[] args) {
        inputPath = args[0];
        spark = SparkSession
                .builder()
                .appName("batch_query_indexer")
                .master("local[*]")
                .getOrCreate();
    }

    public void index() {
        take(10);
    }

    public void take(int rowCount) {
        Dataset<Row> data    = spark.read().parquet(inputPath);
        List<Row>    rows    = data.takeAsList(rowCount);
        String[]     heading = data.columns();
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            Row row = rows.get(rowIndex);
            for (int colIndex = 0; colIndex < row.length(); colIndex++) {
                Object o = row.get(colIndex);
                System.out.println("[" + rowIndex + "][" + colIndex + "]: " + heading[colIndex] + ": " + (o == null ? "<null>" : o.toString()) + ", ");
            }
            System.out.println();
            System.out.println();
        }
        System.out.println();
    }
}
