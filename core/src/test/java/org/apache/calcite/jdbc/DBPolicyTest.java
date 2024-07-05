/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.jdbc;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.*;

import java.io.PrintWriter;
import java.net.URL;
import java.sql.*;
import java.util.Properties;

import org.junit.jupiter.api.Test;

public class DBPolicyTest {

  @Test public void initializeDuckDB() throws Exception {

//        Class.forName("org.apache.calcite.jdbc.Driver");

    if(false){
      Connection conn = DriverManager.getConnection("jdbc:duckdb:/tmp/database");

      // create a table
      Statement stmt2 = conn.createStatement();
      stmt2.execute("CREATE TABLE MY_TABLE_A (id INT, column_name VARCHAR)");
      // insert two items into the table
      stmt2.execute("INSERT INTO MY_TABLE_A (id, column_name) VALUES (1, 'value1'), (2, 'value2')");

      // Get the current schema name
      String currentSchema = conn.getSchema();

      // Print the current schema name
      System.out.println("Current schema: " + currentSchema);

      try (ResultSet rs = stmt2.executeQuery("SELECT * FROM MY_TABLE_A")) {
        while (rs.next()) {
          System.out.println(rs.getInt(1));
          System.out.println(rs.getString(2));
        }
      }
      stmt2.close();

//    stmt.close();
      conn.close();
    }


    //

//    URL modelUrl = DBPolicyTest.class.getClassLoader().getResource("model.json");
//    if (modelUrl == null) {
//      throw new IllegalArgumentException("model.json not found in classpath");
//    }

    Properties info = new Properties();
    info.put("model",
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'main',\n"
            + "  schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'main',\n"
            + "       factory: 'org.apache.calcite.adapter.jdbc.JdbcSchema$Factory',\n"
            + "       operand: {\n"
            + "         jdbcDriver: 'org.duckdb.DuckDBDriver',\n"
            + "         jdbcUrl: 'jdbc:duckdb:/tmp/database'\n"
            + "       }\n"
            + "     }\n"
            + "  ]\n"
            + "}");

    // Connect to Calcite using the model file
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

    Statement stmt = connection.createStatement();

//    try (ResultSet rs = stmt.executeQuery("SELECT * FROM MY_TABLE_A")) {
//      while (rs.next()) {
//        System.out.println(rs.getInt(1));
//        System.out.println(rs.getString(2));
//      }
//    }
//    stmt.close();

    // Get the Calcite connection
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

    String currentCalciteSchema = calciteConnection.getSchema();
    System.out.println("Current schema in Calcite connection: " + currentCalciteSchema);

    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // Create a framework configuration
    FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
        .defaultSchema(rootSchema.getSubSchema("main"))
        .build();

    // Create a planner
    Planner planner = Frameworks.getPlanner(frameworkConfig);

    // Parse the SQL query
    String sql = "SELECT * FROM MY_TABLE_A";
    SqlNode parsedQuery = planner.parse(sql);

    // Validate the SQL query
    SqlNode validatedQuery = planner.validate(parsedQuery);

    // Convert the SQL query to a relational expression
    RelRoot relRoot = planner.rel(validatedQuery);
    RelNode relNode = relRoot.project();

    final RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES, false);
    relNode.explain(relWriter);

    RelRunner runner = connection.unwrap(RelRunner.class);
    PreparedStatement ps = runner.prepareStatement(relNode);
    ResultSet resultSet = ps.executeQuery();

//    System.out.println(((Jdbc41PreparedStatement) ps).toString());

//    PreparedStatement run = RelRunners.run(relNode);
//    ResultSet resultSet = run.executeQuery();



//        Statement statement = calciteConnection.createStatement();

//        String createTableSQL = "CREATE TABLE my_table (id INT, column_name VARCHAR)";
//        statement.execute(createTableSQL);

//        ResultSet resultSet = statement.executeQuery("SELECT * FROM MY_TABLE_A");

    // Process the results
    while (resultSet.next()) {
      System.out.println("ID: " + resultSet.getInt("id") + ", Column: " + resultSet.getString("column_name"));
    }

    // Close the resources
    resultSet.close();
//        statement.close();
    connection.close();

//        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
//
//        // create a table
//        Statement stmt = conn.createStatement();
//        stmt.execute("CREATE TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)");
//        // insert two items into the table
//        stmt.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)");
//
//        CalciteConnection con = conn.unwrap(CalciteConnection.class);
//
//
//
//        try (ResultSet rs = stmt.executeQuery("SELECT * FROM items")) {
//            while (rs.next()) {
//                System.out.println(rs.getString(1));
//                System.out.println(rs.getInt(3));
//            }
//        }
//        stmt.close();
  }
}
