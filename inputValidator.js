const { S3Client, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { StepFunctionsClient, StartExecutionCommand } = require('@aws-sdk/client-stepfunctions');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const s3Client = new S3Client({});
const sfnClient = new StepFunctionsClient({});
const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

// Supported audio formats
const SUPPORTED_FORMATS = ['.mp3', '.wav', '.m4a'];
const MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024; // 5GB

exports.handler = async (event) => {
  console.log('Event:', JSON.stringify(event, null, 2));
  
  // Handle S3 trigger or direct invocation
  let s3Bucket, s3Key;
  
  if (event.Records && event.Records[0].s3) {
    // S3 trigger
    const s3Event = event.Records[0].s3;
    s3Bucket = s3Event.bucket.name;
    s3Key = decodeURIComponent(s3Event.object.key.replace(/\+/g, ' '));
  } else {
    // Direct invocation from Step Functions
    s3Bucket = event.s3Bucket;
    s3Key = event.s3Key;
  }
  
  try {
    // Validate file path
    if (!s3Key.startsWith('raw/') || s3Key.includes('/done/')) {
      console.log('Skipping file not in raw/ folder:', s3Key);
      return { skip: true, reason: 'Not in raw folder' };
    }
    
    // Extract filename and validate format
    const filename = s3Key.split('/').pop();
    const fileExt = filename.substring(filename.lastIndexOf('.')).toLowerCase();
    
    if (!SUPPORTED_FORMATS.includes(fileExt)) {
      throw new Error(`Unsupported file format: ${fileExt}. Supported: ${SUPPORTED_FORMATS.join(', ')}`);
    }
    
    // Get file metadata
    const headCommand = new HeadObjectCommand({
      Bucket: s3Bucket,
      Key: s3Key
    });
    
    const metadata = await s3Client.send(headCommand);
    const fileSizeGB = metadata.ContentLength / (1024 * 1024 * 1024);
    
    // Validate file size
    if (metadata.ContentLength > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${fileSizeGB.toFixed(2)}GB. Maximum: 5GB`);
    }
    
    // Create initial DynamoDB entry
    const timestamp = new Date().toISOString();
    const ttl = Math.floor(Date.now() / 1000) + (30 * 24 * 60 * 60); // 30 days
    
    await docClient.send(new PutCommand({
      TableName: process.env.DYNAMODB_TABLE,
      Item: {
        filename,
        status: 'validating',
        s3Bucket,
        s3Key,
        fileSize: metadata.ContentLength,
        fileSizeGB: parseFloat(fileSizeGB.toFixed(2)),
        format: fileExt,
        timestamp,
        lastUpdated: timestamp,
        ttl
      }
    }));
    
    // Start Step Functions execution if triggered by S3
    if (event.Records) {
      const executionName = `transcription-${filename.replace(/[^a-zA-Z0-9]/g, '-')}-${Date.now()}`;
      
      await sfnClient.send(new StartExecutionCommand({
        stateMachineArn: process.env.STATE_MACHINE_ARN,
        name: executionName,
        input: JSON.stringify({
          s3Bucket,
          s3Key,
          filename,
          validation: {
            fileSize: metadata.ContentLength,
            fileSizeGB: parseFloat(fileSizeGB.toFixed(2)),
            format: fileExt,
            timestamp
          }
        })
      }));
      
      console.log(`Started Step Functions execution: ${executionName}`);
    }
    
    // Return validation results
    return {
      valid: true,
      filename,
      s3Bucket,
      s3Key,
      fileSize: metadata.ContentLength,
      fileSizeGB: parseFloat(fileSizeGB.toFixed(2)),
      format: fileExt
    };
    
  } catch (error) {
    console.error('Validation error:', error);
    
    // Update DynamoDB with error
    if (filename) {
      await docClient.send(new UpdateCommand({
        TableName: process.env.DYNAMODB_TABLE,
        Key: { filename },
        UpdateExpression: 'SET #status = :status, #error = :error, #updated = :updated',
        ExpressionAttributeNames: {
          '#status': 'status',
          '#error': 'error',
          '#updated': 'lastUpdated'
        },
        ExpressionAttributeValues: {
          ':status': 'validation_failed',
          ':error': error.message,
          ':updated': new Date().toISOString()
        }
      }));
    }
    
    // Move file to error folder
    if (s3Key && !s3Key.includes('/error/')) {
      try {
        const copySource = `${s3Bucket}/${s3Key}`;
        const errorKey = s3Key.replace('raw/', 'error/');
        
        await s3Client.send(new CopyObjectCommand({
          Bucket: s3Bucket,
          CopySource: copySource,
          Key: errorKey
        }));
        
        await s3Client.send(new DeleteObjectCommand({
          Bucket: s3Bucket,
          Key: s3Key
        }));
        
        console.log(`Moved invalid file to error folder: ${errorKey}`);
      } catch (moveError) {
        console.error('Error moving file:', moveError);
      }
    }
    
    throw error;
  }
};