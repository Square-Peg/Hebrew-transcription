const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { SSMClient, GetParameterCommand } = require('@aws-sdk/client-ssm');
const nodemailer = require('nodemailer');
const https = require('https');

const s3Client = new S3Client({});
const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const ssmClient = new SSMClient({});

const MAX_ATTACHMENT_SIZE = parseInt(process.env.MAX_ATTACHMENT_SIZE || '10485760'); // 10MB default

/**
 * Get credentials from SSM Parameter Store
 */
async function getCredentialsFromSSM() {
  try {
    const [emailResult, passwordResult] = await Promise.all([
      ssmClient.send(new GetParameterCommand({
        Name: process.env.GMAIL_USER_SSM || '/hebrew-transcription/gmail-user',
        WithDecryption: true
      })),
      ssmClient.send(new GetParameterCommand({
        Name: process.env.GMAIL_PASSWORD_SSM || '/hebrew-transcription/gmail-app-password',
        WithDecryption: true
      }))
    ]);
    
    return {
      user: emailResult.Parameter?.Value,
      pass: passwordResult.Parameter?.Value
    };
  } catch (error) {
    console.error('Error fetching Gmail credentials from SSM:', error);
    // Fall back to environment variables if SSM fails
    return {
      user: process.env.GMAIL_USER,
      pass: process.env.GMAIL_PASSWORD
    };
  }
}

/**
 * Create nodemailer transporter with Gmail
 */
async function createTransporter() {
  const credentials = await getCredentialsFromSSM();
  
  if (!credentials.user || !credentials.pass) {
    throw new Error('Gmail credentials not configured');
  }

  return nodemailer.createTransporter({
    service: 'gmail',
    auth: credentials
  });
}

// Copy all the existing helper functions from emailSender.js
// (formatFileSize, formatDuration, getS3File, getAPIKeyFromSSM, 
// generateAISummary, generateOpenAISummary, formatAISummary, etc.)

/**
 * Main handler
 */
exports.handler = async (event) => {
  console.log('Event:', JSON.stringify(event, null, 2));
  
  const { id, s3Bucket, s3Key, outputKey, transcriptKey, processingTime } = event;
  const filename = s3Key.split('/').pop();
  const recipients = process.env.EMAIL_RECIPIENTS.split(',').map(e => e.trim());
  const fromEmail = process.env.EMAIL_FROM;
  
  try {
    // Get transcript files
    const jsonFile = await getS3File(s3Bucket, outputKey);
    const txtFile = await getS3File(s3Bucket, transcriptKey);
    
    // Generate AI summary
    const txtContent = txtFile.content.toString();
    const aiSummary = await generateAISummary(txtContent);
    const formattedSummary = formatAISummary(aiSummary);
    
    // Determine which files to attach (respect size limits)
    const attachments = [];
    let attachmentNote = '';
    
    if (txtFile.size <= MAX_ATTACHMENT_SIZE) {
      attachments.push({
        filename: transcriptKey.split('/').pop(),
        content: txtFile.content
      });
    } else {
      attachmentNote = `<p><strong>Note:</strong> Text transcript (${formatFileSize(txtFile.size)}) exceeds email size limit. `;
    }
    
    if (jsonFile.size <= MAX_ATTACHMENT_SIZE && attachments.length === 0) {
      attachments.push({
        filename: outputKey.split('/').pop(),
        content: jsonFile.content
      });
    } else if (jsonFile.size > MAX_ATTACHMENT_SIZE) {
      attachmentNote += `JSON file (${formatFileSize(jsonFile.size)}) exceeds email size limit. `;
    }
    
    // Generate S3 pre-signed URLs for files
    const jsonUrl = await getSignedUrl(s3Client, new GetObjectCommand({ Bucket: s3Bucket, Key: outputKey }), { expiresIn: 3600 * 24 * 7 });
    const txtUrl = await getSignedUrl(s3Client, new GetObjectCommand({ Bucket: s3Bucket, Key: transcriptKey }), { expiresIn: 3600 * 24 * 7 });
    
    if (attachmentNote) {
      attachmentNote += `Download files from S3:</p>
        <ul>
          <li><a href="${txtUrl}">Text Transcript</a></li>
          <li><a href="${jsonUrl}">JSON Transcript</a></li>
        </ul>`;
    }
    
    // Create email HTML (reuse the same template)
    const subject = `Hebrew Transcription Complete: ${filename}`;
    const htmlBody = `<!-- Same HTML template as emailSender.js -->`;
    const textBody = `<!-- Same text body as emailSender.js -->`;
    
    // Send email using Gmail
    const transporter = await createTransporter();
    const result = await transporter.sendMail({
      from: fromEmail,
      to: recipients.join(', '),
      subject: subject,
      text: textBody,
      html: htmlBody,
      attachments: attachments
    });
    
    console.log(`Email sent successfully via Gmail: ${result.messageId}`);
    
    // Update DynamoDB
    await docClient.send(new UpdateCommand({
      TableName: process.env.DYNAMODB_TABLE,
      Key: { id },
      UpdateExpression: 'SET #status = :status, #emailSent = :emailSent, #emailSentAt = :emailSentAt',
      ExpressionAttributeNames: {
        '#status': 'status',
        '#emailSent': 'emailSent',
        '#emailSentAt': 'emailSentAt'
      },
      ExpressionAttributeValues: {
        ':status': 'completed',
        ':emailSent': true,
        ':emailSentAt': new Date().toISOString()
      }
    }));
    
    return {
      success: true,
      messageId: result.messageId,
      recipients
    };
    
  } catch (error) {
    console.error('Error sending email:', error);
    
    await docClient.send(new UpdateCommand({
      TableName: process.env.DYNAMODB_TABLE,
      Key: { id },
      UpdateExpression: 'SET #emailError = :error, #updated = :updated',
      ExpressionAttributeNames: {
        '#emailError': 'emailError',
        '#updated': 'lastUpdated'
      },
      ExpressionAttributeValues: {
        ':error': error.message,
        ':updated': new Date().toISOString()
      }
    }));
    
    throw error;
  }
}; 