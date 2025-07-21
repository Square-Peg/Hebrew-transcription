const { SESClient, SendRawEmailCommand } = require('@aws-sdk/client-ses');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { SSMClient, GetParameterCommand } = require('@aws-sdk/client-ssm');
const https = require('https');

const sesClient = new SESClient({ region: process.env.SES_REGION || 'us-east-1' });
const s3Client = new S3Client({});
const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const ssmClient = new SSMClient({});

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
    `From: Hebrew Transcription <${from}>`,
    `To: ${to.join(', ')}`,
    `Reply-To: ${from}`,
    `Subject: ${subject}`,
    'MIME-Version: 1.0',
    'X-Priority: 3',
    'X-Mailer: Hebrew-Transcription-Pipeline',
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
      
      return `<div class="transcript-segment">
        <div class="speaker-label">${speaker}</div>
        <div class="hebrew-text">${text}</div>
        <div class="english-text">${translation}</div>
      </div>`;
    }).join('');
    
    const remaining = segments.length - maxSegments;
    const footer = remaining > 0 ? 
      `<p style="text-align: center; opacity: 0.7; margin-top: 20px;"><em>... and ${remaining} more segments in the full transcript</em></p>` : '';
    
    return preview + footer;
  } catch (error) {
    console.error('Error parsing transcript:', error);
    return '<p style="text-align: center; color: #666;">Unable to generate preview. See attached files for full transcript.</p>';
  }
}

/**
 * Get API key from SSM Parameter Store
 */
async function getAPIKeyFromSSM(parameterName) {
  if (!parameterName) {
    return null;
  }

  try {
    const command = new GetParameterCommand({
      Name: parameterName,
      WithDecryption: true
    });
    
    const response = await ssmClient.send(command);
    return response.Parameter?.Value;
  } catch (error) {
    console.error(`Error fetching SSM parameter ${parameterName}:`, error);
    return null;
  }
}

/**
 * Generate AI summary of transcript
 */
async function generateAISummary(transcriptText) {
  const apiProvider = process.env.AI_PROVIDER || 'openai';
  
  // Try SSM first, then fall back to environment variable
  let apiKey;
  if (process.env.AI_API_KEY_SSM) {
    apiKey = await getAPIKeyFromSSM(process.env.AI_API_KEY_SSM);
  } else {
    apiKey = process.env.AI_API_KEY;
  }
  
  if (!apiKey) {
    console.log('No AI API key configured, skipping summary generation');
    return null;
  }

  try {
    if (apiProvider === 'claude') {
      return await generateClaudeSummary(transcriptText, apiKey);
    } else if (apiProvider === 'openai') {
      return await generateOpenAISummary(transcriptText, apiKey);
    }
  } catch (error) {
    console.error('Error generating AI summary:', error);
    return null;
  }
}

/**
 * Generate summary using Claude API
 */
async function generateClaudeSummary(transcriptText, apiKey) {
  const prompt = `You are Startup-Meeting GPT, a neutral, analytical note-taker for early-stage tech investors. Your input is a verbatim meeting transcript; your output is a short, structured summary capturing problem/solution fit, team quality, traction, fundraising status, risks, and explicit next steps.

REQUIRED OUTPUT HEADERS (use exactly these titles):

**Meeting Snapshot**
One line – date, attendees, and purpose (e.g., "2025-07-15 · Jane Doe & Team ▸ Intro call").

**Startup in One Sentence**
Plain-English elevator pitch, ≤ 30 words.

**Problem & Current Pain**
1–3 bullets on the customer pain and why it matters now.

**Solution & Secret Sauce**
1–3 bullets on how the product solves the pain plus any moat (tech, data, GTM edge).

**Team & Origins**
1–3 bullets on founders' backgrounds, prior wins, and fit for the problem.

**Early Traction / Metrics**
Key figures shared: users, revenue run-rate, retention, burn/runway, pilots, etc.

**Fundraising Status**
Round type, target size, amount committed, valuation guidance, timing.

**Strategic Questions / Risks**
Open issues, competitive threats, or areas where founders seek advice.

**Next Steps & Owners**
Bullet list of concrete follow-ups with owner + due date (e.g., "Send product deck – CEO, by Fri").

STYLE RULES:
- Concise: ~1–3 bullets per header; Meeting Snapshot is one line
- Verbatim accuracy: never invent data; use "— not covered —" for missing sections
- Investor tone: neutral, analytical, present tense ("Company reports 30% MoM growth")
- Numbers first: prefer precise metrics ("$250k ARR") over qualitative adjectives
- Omit pleasantries, small talk, and off-topic banter
- Use the exact header titles above with ** for bold
- No extra commentary outside the specified sections

Transcript:
${transcriptText.substring(0, 15000)}`; // Limit to prevent token overflow

  const data = JSON.stringify({
    model: 'claude-3-sonnet-20240229',
    max_tokens: 1000,
    messages: [
      {
        role: 'user',
        content: prompt
      }
    ]
  });

  const options = {
    hostname: 'api.anthropic.com',
    path: '/v1/messages',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'Content-Length': Buffer.byteLength(data)
    }
  };

  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let responseData = '';
      res.on('data', (chunk) => responseData += chunk);
      res.on('end', () => {
        try {
          const response = JSON.parse(responseData);
          if (response.content && response.content[0]) {
            resolve(response.content[0].text);
          } else {
            reject(new Error('Invalid Claude API response'));
          }
        } catch (error) {
          reject(error);
        }
      });
    });

    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

/**
 * Generate summary using OpenAI Responses API
 */
async function generateOpenAISummary(transcriptText, apiKey) {
  const prompt = `You are Startup-Meeting GPT, a neutral, analytical note-taker for early-stage tech investors. Your input is a verbatim meeting transcript; your output is a short, structured summary capturing problem/solution fit, team quality, traction, fundraising status, risks, and explicit next steps.

REQUIRED OUTPUT HEADERS (use exactly these titles):

**Meeting Snapshot**
One line – date, attendees, and purpose (e.g., "2025-07-15 · Jane Doe & Team ▸ Intro call").

**Startup in One Sentence**
Plain-English elevator pitch, ≤ 30 words.

**Problem & Current Pain**
1–3 bullets on the customer pain and why it matters now.

**Solution & Secret Sauce**
1–3 bullets on how the product solves the pain plus any moat (tech, data, GTM edge).

**Team & Origins**
1–3 bullets on founders' backgrounds, prior wins, and fit for the problem.

**Early Traction / Metrics**
Key figures shared: users, revenue run-rate, retention, burn/runway, pilots, etc.

**Fundraising Status**
Round type, target size, amount committed, valuation guidance, timing.

**Strategic Questions / Risks**
Open issues, competitive threats, or areas where founders seek advice.

**Next Steps & Owners**
Bullet list of concrete follow-ups with owner + due date (e.g., "Send product deck – CEO, by Fri").

STYLE RULES:
- Concise: ~1–3 bullets per header; Meeting Snapshot is one line
- Verbatim accuracy: never invent data; use "— not covered —" for missing sections
- Investor tone: neutral, analytical, present tense ("Company reports 30% MoM growth")
- Numbers first: prefer precise metrics ("$250k ARR") over qualitative adjectives
- Omit pleasantries, small talk, and off-topic banter
- Use the exact header titles above with ** for bold
- No extra commentary outside the specified sections

Transcript:
${transcriptText.substring(0, 15000)}`; // Limit to prevent token overflow

  const data = JSON.stringify({
    model: 'gpt-4o',
    input: prompt,
    temperature: 0.7,
    max_output_tokens: 1000
  });

  const options = {
    hostname: 'api.openai.com',
    path: '/v1/responses',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
      'Content-Length': Buffer.byteLength(data)
    }
  };

  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let responseData = '';
      res.on('data', (chunk) => responseData += chunk);
      res.on('end', () => {
        try {
          const response = JSON.parse(responseData);
          console.log('OpenAI API Response:', JSON.stringify(response, null, 2));
          
          // Handle the new response format
          if (response.output && response.output.length > 0) {
            // Find the first message output
            const messageOutput = response.output.find(item => item.type === 'message');
            if (messageOutput && messageOutput.content && messageOutput.content.length > 0) {
              // Find the output_text content
              const textContent = messageOutput.content.find(content => content.type === 'output_text');
              if (textContent && textContent.text) {
                resolve(textContent.text);
              } else {
                reject(new Error('No text content found in OpenAI response'));
              }
            } else {
              reject(new Error('No message output found in OpenAI response'));
            }
          } else if (response.error) {
            reject(new Error(`OpenAI API error: ${response.error.message || 'Unknown error'}`));
          } else {
            reject(new Error('Invalid OpenAI API response structure'));
          }
        } catch (error) {
          console.error('Error parsing OpenAI response:', error);
          console.error('Raw response:', responseData);
          reject(error);
        }
      });
    });

    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

/**
 * Format AI summary for email
 */
function formatAISummary(summary) {
  if (!summary) {
    return '<p style="text-align: center; color: #666;">AI summary not available. See attached files for full transcript.</p>';
  }

  // Convert markdown-style formatting to HTML with proper structure
  let htmlSummary = summary
    // Convert headers (** text **)
    .replace(/\*\*(.+?)\*\*/g, '<h4 style="font-family: \'Poppins\', sans-serif; font-weight: 700; font-size: 16px; color: #143A39; margin: 20px 0 10px 0; border-bottom: 1px solid #FF8780; padding-bottom: 5px;">$1</h4>')
    // Convert bullet points
    .replace(/^• (.+)$/gm, '<li style="margin: 8px 0; color: #143A39;">$1</li>')
    .replace(/^- (.+)$/gm, '<li style="margin: 8px 0; color: #143A39;">$1</li>')
    // Wrap consecutive <li> elements in <ul>
    .replace(/(<li[^>]*>.*?<\/li>\s*)+/g, '<ul style="margin: 0 0 15px 0; padding-left: 20px;">$&</ul>')
    // Convert line breaks to paragraphs for non-list content
    .split('\n\n')
    .map(block => {
      // Don't wrap headers or lists in paragraphs
      if (block.includes('<h4') || block.includes('<ul') || block.trim() === '') {
        return block;
      }
      return `<p style="margin-bottom: 15px; line-height: 1.6; color: #143A39;">${block.trim()}</p>`;
    })
    .join('\n');

  return htmlSummary;
}

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
    
    // Generate S3 pre-signed URLs for files
    const jsonUrl = await getSignedUrl(s3Client, new GetObjectCommand({ Bucket: s3Bucket, Key: outputKey }), { expiresIn: 3600 * 24 * 7 }); // 7 days
    const txtUrl = await getSignedUrl(s3Client, new GetObjectCommand({ Bucket: s3Bucket, Key: transcriptKey }), { expiresIn: 3600 * 24 * 7 }); // 7 days
    
    if (attachmentNote) {
      attachmentNote += `Download files from S3:</p>
        <ul>
          <li><a href="${txtUrl}">Text Transcript</a></li>
          <li><a href="${jsonUrl}">JSON Transcript</a></li>
        </ul>`;
    }
    
    // Create email content
    const subject = `Meeting Transcription Complete: ${filename}`;
    
    const htmlBody = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;700&display=swap" rel="stylesheet">
  <style>
    body { 
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
      line-height: 1.6; 
      color: #143A39;
      margin: 0;
      padding: 0;
      background-color: #f5f5f5;
    }
    .container { 
      max-width: 600px; 
      margin: 0 auto; 
      background-color: white;
    }
    .header { 
      background-color: #143A39; 
      color: #FAEDDE; 
      padding: 40px 30px; 
      text-align: center; 
    }
    .header h1 {
      font-family: 'Poppins', sans-serif;
      font-weight: 700;
      font-size: 28px;
      margin: 0 0 10px 0;
      letter-spacing: -0.5px;
    }
    .header p {
      font-family: 'Poppins', sans-serif;
      font-weight: 300;
      font-size: 16px;
      margin: 0;
      opacity: 0.9;
    }
    .coral-bar {
      height: 5px;
      background-color: #FF8780;
    }
    .content { 
      padding: 40px 30px;
    }
    .file-name {
      font-family: 'Poppins', sans-serif;
      font-weight: 700;
      font-size: 24px;
      color: #143A39;
      margin: 0 0 30px 0;
    }
    .info-box { 
      background-color: #FAEDDE; 
      padding: 25px; 
      margin: 25px 0; 
      border-radius: 8px;
      border-left: 4px solid #FF8780;
    }
    .info-box h3 {
      font-family: 'Poppins', sans-serif;
      font-weight: 700;
      font-size: 16px;
      color: #143A39;
      margin: 0 0 15px 0;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .info-box ul {
      margin: 0;
      padding-left: 20px;
    }
    .info-box li {
      margin: 8px 0;
      color: #143A39;
    }
    .summary-box { 
      background-color: #FAEDDE; 
      padding: 30px; 
      margin: 30px 0; 
      border-radius: 8px;
      position: relative;
      overflow: hidden;
    }
    .summary-box::before {
      content: '';
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      height: 3px;
      background: linear-gradient(90deg, #FF8780 0%, #143A39 100%);
    }
    .summary-box h3 {
      font-family: 'Poppins', sans-serif;
      font-weight: 700;
      font-size: 18px;
      color: #143A39;
      margin: 0 0 20px 0;
    }
    .summary-content {
      background-color: white;
      padding: 25px;
      border-radius: 6px;
      box-shadow: 0 2px 4px rgba(20, 58, 57, 0.1);
    }
    .summary-content p {
      color: #143A39;
      margin: 0 0 15px 0;
      line-height: 1.6;
    }
    .summary-content p:last-child {
      margin-bottom: 0;
    }
    .button {
      display: inline-block;
      font-family: 'Poppins', sans-serif;
      font-weight: 700;
      padding: 12px 30px;
      background-color: #FF8780;
      color: white;
      text-decoration: none;
      border-radius: 25px;
      margin: 10px 5px;
      text-transform: uppercase;
      font-size: 14px;
      letter-spacing: 0.5px;
      transition: background-color 0.3s;
    }
    .button:hover {
      background-color: #E76D66;
    }
    .footer { 
      background-color: #FAEDDE;
      padding: 30px;
      text-align: center; 
      font-size: 13px; 
      color: #143A39;
    }
    .footer p {
      margin: 5px 0;
    }
    .logo {
      font-family: 'Poppins', sans-serif;
      font-weight: 700;
      font-size: 20px;
      color: #143A39;
      margin-bottom: 10px;
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Meeting Transcription Complete</h1>
      <p>Your startup meeting has been analyzed</p>
    </div>
    <div class="coral-bar"></div>
    <div class="content">
      <h2 class="file-name">${filename}</h2>
      
      <div class="info-box">
        <h3>Processing Details</h3>
        <ul>
          <li><strong>Processing Time:</strong> ${formatDuration(processingTime || 0)}</li>
          <li><strong>Text Transcript:</strong> ${formatFileSize(txtFile.size)}</li>
          <li><strong>JSON Transcript:</strong> ${formatFileSize(jsonFile.size)}</li>
        </ul>
      </div>
      
      ${attachmentNote ? `<div class="info-box">${attachmentNote}</div>` : ''}
      
      <div class="summary-box">
        <h3>Meeting Summary</h3>
        <div class="summary-content">
          ${formattedSummary}
        </div>
      </div>
      
      ${attachments.length > 0 ? 
        '<p style="text-align: center; margin: 30px 0; font-size: 16px;">The full transcript is attached to this email.</p>' : 
        `<p style="text-align: center; margin: 30px 0;">
          <a href="${txtUrl}" class="button">Download Text</a>
          <a href="${jsonUrl}" class="button">Download JSON</a>
        </p>`
      }
    </div>
    
    <div class="footer">
      <div class="logo">Square Peg</div>
      <p>Meeting Transcription Pipeline</p>
      <p style="font-size: 12px; opacity: 0.8;">This is an automated message</p>
    </div>
  </div>
</body>
</html>`;
    
    const textBody = `Meeting Transcription Complete

File: ${filename}
Processing Time: ${formatDuration(processingTime || 0)}

The startup meeting transcription and analysis has been completed successfully.

${attachments.length > 0 ? 'Please see the attached files for the full transcript.' : 
  `Download the transcript files from:
- Text: ${txtUrl}
- JSON: ${jsonUrl}`}

This is an automated message from the Meeting Transcription Pipeline.`;
    
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
      messageId: result.MessageId,
      recipients
    };
    
  } catch (error) {
    console.error('Error sending email:', error);
    
    // Update DynamoDB with error
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