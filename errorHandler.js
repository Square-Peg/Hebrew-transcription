const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');
const { S3Client, CopyObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');

const sesClient = new SESClient({ region: 'us-east-1' });
const s3Client = new S3Client({});
const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

/**
 * Move file to error folder
 */
async function moveFileToError(bucket, key) {
  if (!key || key.includes('/error/')) {
    console.log('File already in error folder or no key provided');
    return null;
  }
  
  try {
    const errorKey = key.replace('raw/', 'error/');
    
    // Copy to error folder
    await s3Client.send(new CopyObjectCommand({
      Bucket: bucket,
      CopySource: `${bucket}/${key}`,
      Key: errorKey,
      MetadataDirective: 'COPY'
    }));
    
    // Delete original
    await s3Client.send(new DeleteObjectCommand({
      Bucket: bucket,
      Key: key
    }));
    
    console.log(`Moved file to error folder: ${errorKey}`);
    return errorKey;
    
  } catch (error) {
    console.error('Error moving file:', error);
    return null;
  }
}

/**
 * Get error details from various sources
 */
async function getErrorDetails(error, filename) {
  const errorInfo = {
    message: 'Unknown error',
    type: 'Unknown',
    details: {},
    timestamp: new Date().toISOString()
  };
  
  // Parse error object
  if (error) {
    if (typeof error === 'string') {
      errorInfo.message = error;
    } else if (error.Error) {
      errorInfo.type = error.Error;
      errorInfo.message = error.Cause || error.Error;
    } else if (error.message) {
      errorInfo.message = error.message;
      errorInfo.type = error.name || 'Error';
    }
    
    // Add any additional error properties
    if (typeof error === 'object') {
      errorInfo.details = {
        ...error,
        stack: error.stack
      };
    }
  }
  
  // Try to get additional context from DynamoDB
  try {
    const dbResult = await docClient.send(new GetCommand({
      TableName: process.env.DYNAMODB_TABLE,
      Key: { filename }
    }));
    
    if (dbResult.Item) {
      errorInfo.processingHistory = {
        status: dbResult.Item.status,
        startTime: dbResult.Item.timestamp,
        lastUpdated: dbResult.Item.lastUpdated,
        instanceInfo: dbResult.Item.instanceInfo
      };
    }
  } catch (dbError) {
    console.error('Error getting DynamoDB info:', dbError);
  }
  
  return errorInfo;
}

/**
 * Send error notification email
 */
async function sendErrorEmail(filename, errorInfo) {
  const recipients = process.env.EMAIL_RECIPIENTS.split(',').map(e => e.trim());
  const fromEmail = process.env.EMAIL_FROM;
  
  const subject = `Hebrew Transcription Failed: ${filename}`;
  
  const htmlBody = `
<!DOCTYPE html>
<html>
<head>
  <style>
    body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
    .container { max-width: 600px; margin: 0 auto; padding: 20px; }
    .header { background-color: #dc3545; color: white; padding: 20px; text-align: center; border-radius: 5px 5px 0 0; }
    .content { background-color: #f9f9f9; padding: 20px; border: 1px solid #ddd; border-radius: 0 0 5px 5px; }
    .error-box { background-color: #fee; padding: 15px; margin: 15px 0; border-radius: 5px; border-left: 4px solid #dc3545; }
    .detail-box { background-color: white; padding: 15px; margin: 15px 0; border-radius: 5px; font-family: monospace; font-size: 12px; }
    .footer { text-align: center; margin-top: 20px; font-size: 12px; color: #666; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Transcription Failed</h1>
    </div>
    <div class="content">
      <h2>File: ${filename}</h2>
      
      <div class="error-box">
        <strong>Error Type:</strong> ${errorInfo.type}<br>
        <strong>Error Message:</strong> ${errorInfo.message}<br>
        <strong>Time:</strong> ${errorInfo.timestamp}
      </div>
      
      ${errorInfo.processingHistory ? `
      <div class="detail-box">
        <strong>Processing History:</strong><br>
        Status: ${errorInfo.processingHistory.status}<br>
        Started: ${errorInfo.processingHistory.startTime}<br>
        Last Updated: ${errorInfo.processingHistory.lastUpdated}<br>
        ${errorInfo.processingHistory.instanceInfo ? 
          `Instance: ${errorInfo.processingHistory.instanceInfo.instanceId} (${errorInfo.processingHistory.instanceInfo.region})` : 
          'No instance information available'}
      </div>
      ` : ''}
      
      <p>The audio file has been moved to the error folder for manual review.</p>
      
      <p><strong>Recommended Actions:</strong></p>
      <ul>
        <li>Check CloudWatch logs for detailed error information</li>
        <li>Verify the audio file format and content</li>
        <li>Check EC2 instance logs if processing started</li>
        <li>Retry processing after resolving the issue</li>
      </ul>
    </div>
    
    <div class="footer">
      <p>This is an automated error notification from the Hebrew Transcription Pipeline.</p>
    </div>
  </div>
</body>
</html>`;
  
  const textBody = `Hebrew Transcription Failed

File: ${filename}
Error Type: ${errorInfo.type}
Error Message: ${errorInfo.message}
Time: ${errorInfo.timestamp}

The audio file has been moved to the error folder for manual review.

Recommended Actions:
- Check CloudWatch logs for detailed error information
- Verify the audio file format and content
- Check EC2 instance logs if processing started
- Retry processing after resolving the issue

This is an automated error notification from the Hebrew Transcription Pipeline.`;
  
  try {
    const command = new SendEmailCommand({
      Destination: {
        ToAddresses: recipients
      },
      Message: {
        Body: {
          Html: { Data: htmlBody },
          Text: { Data: textBody }
        },
        Subject: { Data: subject }
      },
      Source: fromEmail
    });
    
    const result = await sesClient.send(command);
    console.log(`Error notification sent: ${result.MessageId}`);
    return result.MessageId;
    
  } catch (emailError) {
    console.error('Failed to send error email:', emailError);
    return null;
  }
}

/**
 * Main handler
 */
exports.handler = async (event) => {
  console.log('Event:', JSON.stringify(event, null, 2));
  
  const { s3Key, s3Bucket, error } = event;
  const filename = s3Key?.split('/').pop() || 'unknown';
  
  try {
    // Get comprehensive error details
    const errorInfo = await getErrorDetails(error, filename);
    console.log('Error details:', errorInfo);
    
    // Move file to error folder
    let errorKey = null;
    if (s3Key && s3Bucket) {
      errorKey = await moveFileToError(s3Bucket, s3Key);
    }
    
    // Update DynamoDB
    const updateData = {
      status: 'failed',
      error: errorInfo.message,
      errorType: errorInfo.type,
      errorDetails: JSON.stringify(errorInfo.details),
      errorTimestamp: errorInfo.timestamp,
      lastUpdated: errorInfo.timestamp
    };
    
    if (errorKey) {
      updateData.errorKey = errorKey;
    }
    
    const updateExpressions = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};
    
    Object.entries(updateData).forEach(([key, value]) => {
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
    
    // Send error notification
    const emailMessageId = await sendErrorEmail(filename, errorInfo);
    
    console.log(`Error handling complete for ${filename}`);
    
    return {
      success: true,
      filename,
      errorKey,
      errorInfo,
      emailSent: !!emailMessageId
    };
    
  } catch (handlerError) {
    console.error('Error in error handler:', handlerError);
    
    // Even if error handling fails, don't throw
    return {
      success: false,
      filename,
      error: handlerError.message,
      originalError: error
    };
  }
};