package com.amazonaws.samples.bookTable;

/**
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This file is licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License. A copy of
 * the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
*/

import java.io.File;
import java.util.Iterator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class BooksLoadDataToTable {

    public static void main(String[] args) throws Exception {

    	ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/valeriemiliteeva/.aws/credentials), and is in valid format.",
                    e);
        }
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
        	.withCredentials(credentialsProvider)
            .withRegion("ca-central-1")
            .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("Books"); 
        
        //Book 1
        Item item = new Item()
        		.withString("author", "JK Rowling")
        		.withString("title", "Harry Potter and the Sorcerers Stone")
        		.withNumber("year", 1998);
                
        //Book 2
        Item item1 = new Item()
        		.withString("author", "JK Rowling")
        		.withString("title", "Harry Potter and the Chamber of Secrets")
        		.withNumber("year", 1998);
                
        //Book 3
        Item item2 = new Item()
        		.withString("author", "JK Rowling")
        		.withString("title", "Harry Potter and the Prisoner of Azkaban")
        		.withNumber("year", 1999);
                
        //Book 4
        Item item3 = new Item()
        		.withString("author", "JK Rowling")
        		.withString("title", "Harry Potter and the Goblet of Fire")
        		.withNumber("year", 2000);
                
      //Book 5
        Item item4 = new Item()
        		.withString("author", "JK Rowling")
        		.withString("title", "Harry Potter and the Order of the Phoenix")
        		.withNumber("year", 2003);
                
      //Book 6
        Item item5 = new Item()
        		.withString("author", "JK Rowling")
        		.withString("title", "Harry Potter and the Half-Blood Prince")
        		.withNumber("year", 2005);
                
      //Book 7
        Item item6 = new Item()
        		.withString("author", "JK Rowling")
        		.withString("title", "Harry Potter and the Deathly Hallows")
        		.withNumber("year", 2007);
        
        PutItemOutcome outcome = table.putItem(item);
        PutItemOutcome outcome1 = table.putItem(item1);
        PutItemOutcome outcome2 = table.putItem(item2);
        PutItemOutcome outcome3 = table.putItem(item3);
        PutItemOutcome outcome4 = table.putItem(item4);
        PutItemOutcome outcome5 = table.putItem(item5);
        PutItemOutcome outcome6 = table.putItem(item6);
        
        System.out.println("Success");

//        JsonParser parser = new JsonFactory().createParser(new File("moviedata.json"));
//
//        JsonNode rootNode = new ObjectMapper().readTree(parser);
//        Iterator<JsonNode> iter = rootNode.iterator();
//
//        ObjectNode currentNode;
//
//        while (iter.hasNext()) {
//            currentNode = (ObjectNode) iter.next();
//
//            int year = currentNode.path("year").asInt();
//            String title = currentNode.path("title").asText();
//
//            try {
//                table.putItem(new Item().withPrimaryKey("year", year, "title", title).withJSON("info",
//                    currentNode.path("info").toString()));
//                System.out.println("PutItem succeeded: " + year + " " + title);
//
//            }
//            catch (Exception e) {
//                System.err.println("Unable to add movie: " + year + " " + title);
//                System.err.println(e.getMessage());
//                break;
//            }
//        }
//        parser.close();
    }
}
