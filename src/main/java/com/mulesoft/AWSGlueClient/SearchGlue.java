package com.mulesoft.AWSGlueClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;

public class SearchGlue {
	
    public static void main(String[] args) {
    	
    	searchGlueByDatabase("AKIAUTMDBSI2O7BHDGVA","O6wh3zTJHAC3OebA/SLRRzyNm9QY4rpOCj7H6v4M", "us-east-1", "datacatalogdb");
    }
    
    public static GlueClient init (String awsAccessKey, String awsSecretKey, String inputRegion) {
    	//Set AWS region
    	Region region;
    	
    	if (inputRegion != null) 
    		region = Region.of(inputRegion);
    	else
    		region = Region.of("us-east-1");
    	
    	//Initialize GlueClient with AWS Credentials
    	GlueClient glueClient = GlueClient.builder()
    			.region(region)
    			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(awsAccessKey,awsSecretKey)))
    			.build();
    	
    	return glueClient;
    }

    public static void close (GlueClient glueClient) {
    	glueClient.close();
    }
    
    public static String searchGlueByDatabase(String awsAccessKey, String awsSecretKey, String inputRegion, String databaseName) {
    	
    	//Initialize GlueClient
    	GlueClient glueClient = init(awsAccessKey, awsSecretKey, inputRegion);
    	
    	//Prepare Database search request
    	GetTablesRequest getTablesRequest = GetTablesRequest.builder().databaseName(databaseName).build();
    	
    	//Invoke glue client by database name
    	GetTablesResponse getTablesResponse = glueClient.getTables(getTablesRequest);
    	
    	if (getTablesResponse != null) {
    		System.out.println(getTablesResponse.toString());
    	}
    	
    	//Close glue client
    	close(glueClient);
    	
    	//Convert response to JSON
        ObjectMapper mapper = new ObjectMapper()
        		.registerModule(new JavaTimeModule());
        
        String getTablesResponseJson = null;
        try {
        	getTablesResponseJson = mapper.writeValueAsString(getTablesResponse.toBuilder());
        } catch (Exception e) {
        	System.out.println("GlueClent --- Exception while converting database response to JSON");
        	e.printStackTrace();
        }
        System.out.println(getTablesResponseJson);
        
        //Return JSON
		return getTablesResponseJson;
    	
    }
}