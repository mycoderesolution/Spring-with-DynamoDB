package com.mycoderesolutions.dynamo.repository;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.mycoderesolutions.dynamo.model.Student;

@Repository
public class DynamoDBRepository {
	
	private Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private DynamoDBMapper dynamoDBMapper;
	
	
	public void insertIntoDynamoDB(Student student) {
		dynamoDBMapper.save(student);
	}
	
	public Student getOneStudentDetail(String studentId, String lastName) {
		return dynamoDBMapper.load(Student.class, studentId, lastName);
	}
	
	public void updateStudentDetails(Student student) {
		try {
			dynamoDBMapper.save(student, buildDynamoDBSaveExpression(student));
		}catch(ConditionalCheckFailedException checkFailedException) {
			LOGGER.error("Invalid data - " + checkFailedException.getErrorMessage());
		}
	}	
	
	private DynamoDBSaveExpression buildDynamoDBSaveExpression(Student student) {
		DynamoDBSaveExpression dynamoDBSaveExpression = new DynamoDBSaveExpression();
		Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
		expected.put("studentId", new ExpectedAttributeValue(new AttributeValue(student.getStudentId())).withComparisonOperator(ComparisonOperator.EQ));
		dynamoDBSaveExpression.setExpected(expected);
		return dynamoDBSaveExpression;
	}

	public void deleteStudentDetails(Student student) {
		dynamoDBMapper.delete(student);
	}
	
}
