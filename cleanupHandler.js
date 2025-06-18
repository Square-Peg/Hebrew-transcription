const { EC2Client, TerminateInstancesCommand, DescribeInstancesCommand } = require('@aws-sdk/client-ec2');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

// Regional configuration
const REGIONS = ['us-east-1', 'us-west-2', 'ap-southeast-2'];

/**
 * Terminate EC2 instance
 */
async function terminateInstance(instanceId, region) {
  if (!instanceId || !region) {
    console.log('No instance to terminate');
    return { terminated: false };
  }
  
  const ec2Client = new EC2Client({ region });
  
  try {
    // First check if instance exists and is running
    const describeCommand = new DescribeInstancesCommand({
      InstanceIds: [instanceId]
    });
    
    const describeResult = await ec2Client.send(describeCommand);
    const instance = describeResult.Reservations?.[0]?.Instances?.[0];
    
    if (!instance) {
      console.log(`Instance ${instanceId} not found in ${region}`);
      return { terminated: false, reason: 'Instance not found' };
    }
    
    const state = instance.State.Name;
    if (['terminated', 'terminating'].includes(state)) {
      console.log(`Instance ${instanceId} already ${state}`);
      return { terminated: true, alreadyTerminated: true };
    }
    
    // Terminate the instance
    console.log(`Terminating instance ${instanceId} in ${region}...`);
    const terminateCommand = new TerminateInstancesCommand({
      InstanceIds: [instanceId]
    });
    
    const terminateResult = await ec2Client.send(terminateCommand);
    const newState = terminateResult.TerminatingInstances[0].CurrentState.Name;
    
    console.log(`✅ Instance ${instanceId} is now ${newState}`);
    return { 
      terminated: true, 
      previousState: state,
      currentState: newState 
    };
    
  } catch (error) {
    console.error(`Error terminating instance ${instanceId}:`, error);
    return { terminated: false, error: error.message };
  }
}

/**
 * Clean up any orphaned instances for this audio file
 */
async function cleanupOrphanedInstances(filename) {
  const filenameHash = require('crypto').createHash('md5').update(filename).digest('hex').substring(0, 8);
  const results = [];
  
  for (const region of REGIONS) {
    const ec2Client = new EC2Client({ region });
    
    try {
      // Find instances tagged with this audio file
      const describeCommand = new DescribeInstancesCommand({
        Filters: [
          {
            Name: 'tag:AudioFile',
            Values: [filename]
          },
          {
            Name: 'instance-state-name',
            Values: ['running', 'pending', 'stopping', 'stopped']
          }
        ]
      });
      
      const describeResult = await ec2Client.send(describeCommand);
      const instances = describeResult.Reservations?.flatMap(r => r.Instances) || [];
      
      for (const instance of instances) {
        console.log(`Found orphaned instance ${instance.InstanceId} in ${region}`);
        const terminateResult = await terminateInstance(instance.InstanceId, region);
        results.push({
          instanceId: instance.InstanceId,
          region,
          ...terminateResult
        });
      }
    } catch (error) {
      console.error(`Error checking for orphaned instances in ${region}:`, error);
    }
  }
  
  return results;
}

/**
 * Update final status in DynamoDB
 */
async function updateFinalStatus(filename, success, additionalData = {}) {
  const timestamp = new Date().toISOString();
  
  const updates = {
    status: success ? 'completed' : 'failed',
    lastUpdated: timestamp,
    cleanupCompleted: true,
    cleanupTimestamp: timestamp,
    ...additionalData
  };
  
  const updateExpressions = [];
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};
  
  Object.entries(updates).forEach(([key, value]) => {
    updateExpressions.push(`#${key} = :${key}`);
    expressionAttributeNames[`#${key}`] = key;
    expressionAttributeValues[`:${key}`] = value;
  });
  
  await docClient.send(new UpdateCommand({
    TableName: process.env.DYNAMODB_TABLE,
    Key: { filename },
    UpdateExpression: `SET ${updateExpressions.join(', ')}`,
    ExpressionAttributeNames: expressionAttributeNames,
    ExpressionAttributeValues: expressionAttributeValues
  }));
}

/**
 * Main handler
 */
exports.handler = async (event) => {
  console.log('Event:', JSON.stringify(event, null, 2));
  
  const { s3Key, s3Bucket, success, instanceId, region } = event;
  const filename = s3Key.split('/').pop();
  
  try {
    const cleanupResults = {
      instanceTerminated: false,
      orphanedInstancesCleaned: [],
      errors: []
    };
    
    // Terminate the main instance if provided
    if (instanceId && region) {
      console.log(`Terminating primary instance ${instanceId} in ${region}...`);
      const terminateResult = await terminateInstance(instanceId, region);
      cleanupResults.instanceTerminated = terminateResult.terminated;
      
      if (!terminateResult.terminated && !terminateResult.alreadyTerminated) {
        cleanupResults.errors.push(`Failed to terminate instance: ${terminateResult.error || 'Unknown error'}`);
      }
    }
    
    // Clean up any orphaned instances
    console.log('Checking for orphaned instances...');
    const orphanedResults = await cleanupOrphanedInstances(filename);
    cleanupResults.orphanedInstancesCleaned = orphanedResults;
    
    // Update final status in DynamoDB
    await updateFinalStatus(filename, success, {
      cleanupResults: JSON.stringify(cleanupResults)
    });
    
    // Log summary
    const totalCleaned = orphanedResults.filter(r => r.terminated).length;
    console.log(`Cleanup complete:
      - Primary instance terminated: ${cleanupResults.instanceTerminated}
      - Orphaned instances cleaned: ${totalCleaned}
      - Final status: ${success ? 'SUCCESS' : 'FAILED'}`);
    
    return {
      success: true,
      filename,
      cleanupResults
    };
    
  } catch (error) {
    console.error('Error during cleanup:', error);
    
    // Try to update DynamoDB even if cleanup fails
    try {
      await updateFinalStatus(filename, false, {
        cleanupError: error.message
      });
    } catch (dbError) {
      console.error('Error updating DynamoDB:', dbError);
    }
    
    // Don't throw - cleanup errors shouldn't fail the pipeline
    return {
      success: false,
      filename,
      error: error.message
    };
  }
};