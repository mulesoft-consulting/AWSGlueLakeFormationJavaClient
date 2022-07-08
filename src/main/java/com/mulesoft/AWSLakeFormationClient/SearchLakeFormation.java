package com.mulesoft.AWSLakeFormationClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.LFTag;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationResponse;
import software.amazon.awssdk.services.lakeformation.model.SearchDatabasesByLfTagsRequest;
import software.amazon.awssdk.services.lakeformation.model.SearchDatabasesByLfTagsResponse;
import software.amazon.awssdk.services.lakeformation.model.SearchTablesByLfTagsRequest;
import software.amazon.awssdk.services.lakeformation.model.SearchTablesByLfTagsResponse;
import software.amazon.awssdk.services.lakeformation.model.TaggedDatabase;

public class SearchLakeFormation {

    public static void main(String[] args) {

    	searchLakeFormationDatabaseByLFTags("AKIAUTMDBSI2O7BHDGVA","O6wh3zTJHAC3OebA/SLRRzyNm9QY4rpOCj7H6v4M", "us-east-1", "ailment-field", "true");
    	searchLakeFormationTablesByLFTags("AKIAUTMDBSI2O7BHDGVA","O6wh3zTJHAC3OebA/SLRRzyNm9QY4rpOCj7H6v4M", "us-east-1", "ailment-field", "true");
    }

    public static LakeFormationClient init (String awsAccessKey, String awsSecretKey, String inputRegion) {
    	
    	//Set AWS region
    	Region region;
    	if (inputRegion != null) 
    		region = Region.of(inputRegion);
    	else
    		region = Region.of("us-east-1");
    	
    	//Initialize LakeFormationClient with AWS Credentials
        LakeFormationClient lakeFormationClient = LakeFormationClient.builder()
        		.region(region)
        		.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(awsAccessKey,awsSecretKey)))
        		.build();
        
        return lakeFormationClient;
    }
    
    public static void close (LakeFormationClient lakeFormationClient) {
    	lakeFormationClient.close();
    }
    
    public static String searchLakeFormationDatabaseByLFTags(String awsAccessKey, String awsSecretKey, String inputRegion, String tagKey, String tagValues) {
    	
    	//Initialize LakeFormationClient 
    	LakeFormationClient lakeFormationClient = init(awsAccessKey, awsSecretKey, inputRegion);
    	
    	//Create LFTags list to search for
    	List<LFTag> lfTagsList = new ArrayList<LFTag>();
    	LFTag lfTag = LFTag.builder().tagKey(tagKey).tagValues(Arrays.asList(tagValues.split(","))).build();
    	lfTagsList.add(lfTag);
    	
    	//Prepare search request
    	SearchDatabasesByLfTagsRequest searchDatabaseRequest = SearchDatabasesByLfTagsRequest.builder().expression(lfTagsList).build();
    	
    	//Invoke searchDatabasesByLFTags method on lakeFormationClient and retrieve response 
    	SearchDatabasesByLfTagsResponse searchDatabaseResponse = lakeFormationClient.searchDatabasesByLFTags(searchDatabaseRequest);
    	
    	if (searchDatabaseResponse != null) {
    		System.out.println(searchDatabaseResponse.toString());
    	}
    	
    	//Close LakeFormationClient
        close(lakeFormationClient);  	
    	
        //Convert response to JSON
        ObjectMapper mapper = new ObjectMapper();
        String databaseResponseJson = null;
        try {
        	databaseResponseJson = mapper.writeValueAsString(searchDatabaseResponse.toBuilder());
        } catch (Exception e) {
        	System.out.println("Exception while converting database response to JSON");
        	e.printStackTrace();
        }
        System.out.println(databaseResponseJson);
        
        //Return JSON
		return databaseResponseJson;
    }
    
    public static String searchLakeFormationTablesByLFTags(String awsAccessKey, String awsSecretKey, String inputRegion, String tagKey, String tagValues) {
    	
    	//Initialize LakeFormationClient
    	LakeFormationClient lakeFormationClient = init(awsAccessKey, awsSecretKey, inputRegion);
    	
    	//Create LFTags list to search for
    	List<LFTag> lfTagsList = new ArrayList<LFTag>();
    	LFTag lfTag = LFTag.builder().tagKey(tagKey).tagValues(Arrays.asList(tagValues.split(","))).build();
    	lfTagsList.add(lfTag);
    	
    	//Prepare search request
    	SearchTablesByLfTagsRequest searchTablesRequest = SearchTablesByLfTagsRequest.builder().expression(lfTagsList).build();
    	
    	//Invoke searchTablesByLFTags method on lakeFormationClient and retrieve response
    	SearchTablesByLfTagsResponse searchTablesResponse = lakeFormationClient.searchTablesByLFTags(searchTablesRequest);
    	
    	if (searchTablesResponse != null) {
    		System.out.println(searchTablesResponse.toString());
    	}
    	
    	//Close LakeFormationClient
    	close(lakeFormationClient);
    	
    	//Convert response to JSON
    	ObjectMapper mapper = new ObjectMapper();
        String tablesResponseJson = null;
        try {
        	tablesResponseJson = mapper.writeValueAsString(searchTablesResponse.toBuilder());
        } catch (Exception e) {
        	System.out.println("Exception while converting tables response to JSON");
        	e.printStackTrace();
        }
        System.out.println(tablesResponseJson);
        
        //Return JSON
		return tablesResponseJson;
    }
}