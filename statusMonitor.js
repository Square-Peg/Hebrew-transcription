const { S3Client, HeadObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const { EC2Client, DescribeInstancesCommand } = require('@aws-sdk/client-ec2');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');

const s3Client = new S3Client({});
const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

/**
 * Check if processing is complete by looking for output files
 */
async function checkOutputFiles(s3Bucket, filename) {
  const baseFilename = filename.replace(/\.(mp3|wav|m4a)$/, '');
  const outputPrefix = `outputs/${baseFilename}`;
  
  try {
    // Check for transcript files
    const listCommand = new ListObjectsV2Command({
      Bucket: s3Bucket,
      Prefix: outputPrefix,
      MaxKeys: 10
    });
    
    const listResult = await s3Client.send(listCommand);
    
    if (!listResult.Contents || listResult.Contents.length === 0) {
      return { found: false };
    }
    
    // Look for expected output files
    const files = listResult.Contents.map(obj => obj.Key);
    const jsonFile = files.find(f => f.endsWith('_transcript.json'));
    const txtFile = files.find(f => f.endsWith('_transcript.txt'));
    
    if (jsonFile && txtFile) {
      // Get file sizes and timestamps
      const jsonMeta = await s3Client.send(new HeadObjectCommand({
        Bucket: s3Bucket,
        Key: jsonFile
      }));
      
      return {
        found: true,
        outputKey: jsonFile,
        transcriptKey: txtFile,
        outputSize: jsonMeta.ContentLength,
        completedAt: jsonMeta.LastModified
      };
    }
    
    return { found: false };
  } catch (error) {
    console.error('Error checking output files:', error);
    return { found: false, error: error.message };
  }
}

/**
 * Check if instance is still running
 */
async function checkInstanceStatus(instanceId, region) {
  if (!instanceId || !region) {
    return { running: false };
  }
  
  const ec2Client = new EC2Client({ region });
  
  try {
    const command = new DescribeInstancesCommand({
      InstanceIds: [instanceId]
    });
    
    const result = await ec2Client.send(command);
    const instance = result.Reservations?.[0]?.Instances?.[0];
    
    if (!instance) {
      return { running: false };
    }
    
    return {
      running: ['running', 'pending'].includes(instance.State.Name),
      state: instance.State.Name,
      launchTime: instance.LaunchTime,
      publicIp: instance.PublicIpAddress
    };
  } catch (error) {
    console.error('Error checking instance status:', error);
    return { running: false, error: error.message };
  }
}

/**
 * Check if input file has been moved to done folder
 */
async function checkInputFileMoved(s3Bucket, s3Key) {
  const doneKey = s3Key.replace('raw/', 'raw/done/');
  
  try {
    await s3Client.send(new HeadObjectCommand({
      Bucket: s3Bucket,
      Key: doneKey
    }));
    return true;
  } catch (error) {
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      return false;
    }
    throw error;
  }
}

/**
 * Get processing status from DynamoDB
 */
async function getDynamoDBStatus(filename) {
  try {
    const result = await docClient.send(new GetCommand({
      TableName: process.env.DYNAMODB_TABLE,
      Key: { filename }
    }));
    
    return result.Item || null;
  } catch (error) {
    console.error('Error getting DynamoDB status:', error);
    return null;
  }
}

/**
 * Update DynamoDB status
 */
async function updateDynamoDBStatus(filename, updates) {
  const timestamp = new Date().toISOString();
  
  const updateExpressions = ['#updated = :updated'];
  const expressionAttributeNames = { '#updated': 'lastUpdated' };
  const expressionAttributeValues = { ':updated': timestamp };
  
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
  
  const { s3Bucket, s3Key, instanceId } = event;
  const filename = s3Key.split('/').pop();
  
  try {
    // Get current status from DynamoDB
    const dbStatus = await getDynamoDBStatus(filename);
    const startTime = dbStatus?.timestamp ? new Date(dbStatus.timestamp) : new Date();
    const elapsedMinutes = Math.floor((Date.now() - startTime.getTime()) / (1000 * 60));
    
    // Check various completion indicators
    const outputCheck = await checkOutputFiles(s3Bucket, filename);
    const inputMoved = await checkInputFileMoved(s3Bucket, s3Key);
    
    // Check instance status if provided
    let instanceStatus = { running: false };
    if (instanceId && dbStatus?.instanceInfo?.region) {
      instanceStatus = await checkInstanceStatus(instanceId, dbStatus.instanceInfo.region);
    }
    
    // Determine overall status
    let status = {
      isComplete: false,
      isFailed: false,
      elapsedMinutes,
      instanceRunning: instanceStatus.running,
      outputsFound: outputCheck.found,
      inputMoved
    };
    
    // Success: outputs found and input moved
    if (outputCheck.found && inputMoved) {
      status.isComplete = true;
      status.outputKey = outputCheck.outputKey;
      status.transcriptKey = outputCheck.transcriptKey;
      status.processingTime = elapsedMinutes;
      
      await updateDynamoDBStatus(filename, {
        status: 'completed',
        outputKey: outputCheck.outputKey,
        transcriptKey: outputCheck.transcriptKey,
        completedAt: outputCheck.completedAt,
        processingTimeMinutes: elapsedMinutes
      });
      
      console.log(`✅ Processing complete for ${filename}`);
      return status;
    }
    
    // Still processing
    if (instanceStatus.running) {
      console.log(`⏳ Instance still running for ${filename} (${elapsedMinutes} minutes elapsed)`);
      
      await updateDynamoDBStatus(filename, {
        status: 'processing',
        elapsedMinutes
      });
      
      return status;
    }
    
    // Instance stopped but no outputs - check if it's a failure
    if (!instanceStatus.running && elapsedMinutes > 5) {
      // Check if file was moved to error folder
      const errorKey = s3Key.replace('raw/', 'error/');
      try {
        await s3Client.send(new HeadObjectCommand({
          Bucket: s3Bucket,
          Key: errorKey
        }));
        
        status.isFailed = true;
        status.error = 'File moved to error folder';
        
        await updateDynamoDBStatus(filename, {
          status: 'failed',
          error: 'Processing failed - file in error folder'
        });
        
        console.error(`❌ Processing failed for ${filename} - file in error folder`);
        return status;
      } catch (error) {
        // Not in error folder
      }
      
      // If instance stopped without outputs after 5 minutes, likely failed
      if (elapsedMinutes > 5) {
        status.isFailed = true;
        status.error = 'Instance stopped without producing outputs';
        
        await updateDynamoDBStatus(filename, {
          status: 'failed',
          error: 'Instance terminated without outputs'
        });
        
        console.error(`❌ Processing failed for ${filename} - no outputs`);
        return status;
      }
    }
    
    // Still waiting
    console.log(`⏳ Waiting for processing to complete for ${filename} (${elapsedMinutes} minutes elapsed)`);
    return status;
    
  } catch (error) {
    console.error('Error monitoring status:', error);
    
    await updateDynamoDBStatus(filename, {
      status: 'monitoring_error',
      error: error.message
    });
    
    return {
      isComplete: false,
      isFailed: true,
      error: error.message,
      elapsedMinutes: 0
    };
  }
};