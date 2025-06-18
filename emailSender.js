const { SESClient, SendRawEmailCommand } = require('@aws-sdk/client-ses');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');

const sesClient = new SESClient({ region: 'us-east-1' }); // SES is often in us-east-1
const s3Client = new S3Client({});
const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const MAX_ATTACHMENT_SIZE = parseInt(process.env.MAX_ATTACHMENT_SIZE || '10485760'); // 10MB default

/**
 * Format file size for display
 */
function formatFileSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
  return (bytes / (1024 * 1024 * 1024)).toFixed(1) + ' GB';
}

/**
 * Format processing time
 */
function formatDuration(minutes) {
  if (minutes < 60) return `${minutes} minutes`;
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return `${hours}h ${mins}m`;
}

/**
 * Get file from S3
 */
async function getS3File(bucket, key) {
  try {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await s3Client.send(command);
    
    // Convert stream to string
    const chunks = [];
    for await (const chunk of response.Body) {
      chunks.push(chunk);
    }
    const buffer = Buffer.concat(chunks);
    
    return {
      content: buffer,
      contentType: response.ContentType || 'application/octet-stream',
      size: response.ContentLength
    };
  } catch (error) {
    console.error(`Error getting S3 file ${key}:`, error);
    throw error;
  }
}

/**
 * Create MIME email with attachments
 */
function createMimeEmail(from, to, subject, htmlBody, textBody, attachments = []) {
  const boundary = `----=_Part_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  
  let message = [
    `From: ${from}`,
    `To: ${to.join(', ')}`,
    `Subject: ${subject}`,
    'MIME-Version: 1.0',
    `Content-Type: multipart/mixed; boundary="${boundary}"`,
    '',
    `--${boundary}`,
    'Content-Type: multipart/alternative; boundary="alt-' + boundary + '"',
    '',
    '--alt-' + boundary,
    'Content-Type: text/plain; charset=UTF-8',
    'Content-Transfer-Encoding: 7bit',
    '',
    textBody,
    '',
    '--alt-' + boundary,
    'Content-Type: text/html; charset=UTF-8',
    'Content-Transfer-Encoding: 7bit',
    '',
    htmlBody,
    '',
    '--alt-' + boundary + '--'
  ];
  
  // Add attachments
  for (const attachment of attachments) {
    message.push(
      '',
      `--${boundary}`,
      `Content-Type: ${attachment.contentType}; name="${attachment.filename}"`,
      'Content-Transfer-Encoding: base64',
      `Content-Disposition: attachment; filename="${attachment.filename}"`,
      '',
      attachment.content.toString('base64')
    );
  }
  
  message.push('', `--${boundary}--`);
  
  return message.join('\r\n');
}

/**
 * Extract transcript preview from JSON
 */
function getTranscriptPreview(transcriptJson, maxSegments = 5) {
  try {
    const data = JSON.parse(transcriptJson);
    const segments = data.grouped_segments || data.segments || [];
    
    const preview = segments.slice(0, maxSegments).map(seg => {
      const speaker = seg.speaker || 'Unknown';
      const text = seg.text || '';
      const translation = seg.translation || '';
      
      return `<div style="margin-bottom: 15px;">
        <strong>${speaker}:</strong><br>
        Hebrew: ${text}<br>
        English: <em>${translation}</em>
      </div>`;
    }).join('');
    
    const remaining = segments.length - maxSegments;
    const footer = remaining > 0 ? 
      `<p><em>... and ${remaining} more segments in the full transcript</em></p>` : '';
    
    return preview + footer;
  } catch (error) {
    console.error('Error parsing transcript:', error);
    return '<p>Unable to generate preview. See attached files for full transcript.</p>';
  }
}

/**
 * Main handler
 */
exports.handler = async (event) => {
  console.log('Event:', JSON.stringify(event, null, 2));
  
  const { s3Bucket, s3Key, outputKey, transcriptKey, processingTime } = event;
  const filename = s3Key.split('/').pop();
  const recipients = process.env.EMAIL_RECIPIENTS.split(',').map(e => e.trim());
  const fromEmail = process.env.EMAIL_FROM;
  
  try {
    // Get transcript files
    const jsonFile = await getS3File(s3Bucket, outputKey);
    const txtFile = await getS3File(s3Bucket, transcriptKey);
    
    // Generate transcript preview
    const transcriptPreview = getTranscriptPreview(jsonFile.content.toString());
    
    // Determine which files to attach (respect size limits)
    const attachments = [];
    let attachmentNote = '';
    
    if (txtFile.size <= MAX_ATTACHMENT_SIZE) {
      attachments.push({
        filename: transcriptKey.split('/').pop(),
        content: txtFile.content,
        contentType: 'text/plain'
      });
    } else {
      attachmentNote = `<p><strong>Note:</strong> Text transcript (${formatFileSize(txtFile.size)}) exceeds email size limit. `;
    }
    
    if (jsonFile.size <= MAX_ATTACHMENT_SIZE && attachments.length === 0) {
      attachments.push({
        filename: outputKey.split('/').pop(),
        content: jsonFile.content,
        contentType: 'application/json'
      });
    } else if (jsonFile.size > MAX_ATTACHMENT_SIZE) {
      attachmentNote += `JSON file (${formatFileSize(jsonFile.size)}) exceeds email size limit. `;
    }
    
    // Generate S3 URLs for files
    const baseUrl = `https://${s3Bucket}.s3.amazonaws.com`;
    const jsonUrl = `${baseUrl}/${outputKey}`;
    const txtUrl = `${baseUrl}/${transcriptKey}`;
    
    if (attachmentNote) {
      attachmentNote += `Download files from S3:</p>
        <ul>
          <li><a href="${txtUrl}">Text Transcript</a></li>
          <li><a href="${jsonUrl}">JSON Transcript</a></li>
        </ul>`;
    }
    
    // Create email content
    const subject = `Hebrew Transcription Complete: ${filename}`;
    
    const htmlBody = `
<!DOCTYPE html>
<html>
<head>
  <style>
    body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
    .container { max-width: 600px; margin: 0 auto; padding: 20px; }
    .header { background-color: #0073e6; color: white; padding: 20px; text-align: center; border-radius: 5px 5px 0 0; }
    .content { background-color: #f9f9f9; padding: 20px; border: 1px solid #ddd; border-radius: 0 0 5px 5px; }
    .info-box { background-color: white; padding: 15px; margin: 15px 0; border-radius: 5px; border-left: 4px solid #0073e6; }
    .transcript-preview { background-color: white; padding: 20px; margin: 20px 0; border-radius: 5px; border: 1px solid #e0e0e0; }
    .footer { text-align: center; margin-top: 20px; font-size: 12px; color: #666; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Hebrew Transcription Complete</h1>
    </div>
    <div class="content">
      <h2>File: ${filename}</h2>
      
      <div class="info-box">
        <strong>Processing Details:</strong>
        <ul>
          <li>Processing Time: ${formatDuration(processingTime || 0)}</li>
          <li>Output Files:
            <ul>
              <li>Text Transcript: ${formatFileSize(txtFile.size)}</li>
              <li>JSON Transcript: ${formatFileSize(jsonFile.size)}</li>
            </ul>
          </li>
        </ul>
      </div>
      
      ${attachmentNote ? `<div class="info-box">${attachmentNote}</div>` : ''}
      
      <div class="transcript-preview">
        <h3>Transcript Preview:</h3>
        ${transcriptPreview}
      </div>
      
      <p>The full transcript ${attachments.length > 0 ? 'is attached to this email' : 'can be downloaded from the links above'}.</p>
    </div>
    
    <div class="footer">
      <p>This is an automated message from the Hebrew Transcription Pipeline.</p>
    </div>
  </div>
</body>
</html>`;
    
    const textBody = `Hebrew Transcription Complete

File: ${filename}
Processing Time: ${formatDuration(processingTime || 0)}

The transcription has been completed successfully.

${attachments.length > 0 ? 'Please see the attached files for the full transcript.' : 
  `Download the transcript files from:
- Text: ${txtUrl}
- JSON: ${jsonUrl}`}

This is an automated message from the Hebrew Transcription Pipeline.`;
    
    // Create and send email
    const mimeEmail = createMimeEmail(fromEmail, recipients, subject, htmlBody, textBody, attachments);
    
    const command = new SendRawEmailCommand({
      RawMessage: {
        Data: Buffer.from(mimeEmail)
      }
    });
    
    const result = await sesClient.send(command);
    console.log(`Email sent successfully: ${result.MessageId}`);
    
    // Update DynamoDB
    await docClient.send(new UpdateCommand({
      TableName: process.env.DYNAMODB_TABLE,
      Key: { filename },
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
      messageId: result.MessageId,
      recipients
    };
    
  } catch (error) {
    console.error('Error sending email:', error);
    
    // Update DynamoDB with error
    await docClient.send(new UpdateCommand({
      TableName: process.env.DYNAMODB_TABLE,
      Key: { filename },
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