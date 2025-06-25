const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const s3Client = new S3Client({ region: process.env.AWS_REGION });

/**
 * Generate pre-signed URL for Hebrew audio file uploads
 * Used by Audio Hijack to upload recordings directly to S3
 */
exports.handler = async (event) => {
  console.log('Hebrew Audio Upload URL Request:', JSON.stringify(event, null, 2));
  
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,X-Api-Key',
    'Access-Control-Allow-Methods': 'OPTIONS,POST'
  };
  
  // Handle preflight requests
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: ''
    };
  }
  
  try {
    // Parse request body
    const body = JSON.parse(event.body);
    const { filename, fileSizeBytes, metadata = {} } = body;
    
    // Validate filename
    if (!filename || typeof filename !== 'string') {
      return {
        statusCode: 400,
        headers: corsHeaders,
        body: JSON.stringify({ 
          error: 'Filename is required' 
        })
      };
    }
    
    // Validate file extension
    const validExtensions = ['.mp3', '.wav', '.m4a'];
    const fileExt = filename.toLowerCase().substring(filename.lastIndexOf('.'));
    
    if (!validExtensions.includes(fileExt)) {
      return {
        statusCode: 400,
        headers: corsHeaders,
        body: JSON.stringify({ 
          error: `Invalid file type. Supported formats: ${validExtensions.join(', ')}` 
        })
      };
    }
    
    // Validate file size (5GB max)
    const maxSize = 5 * 1024 * 1024 * 1024; // 5GB
    if (fileSizeBytes && fileSizeBytes > maxSize) {
      return {
        statusCode: 400,
        headers: corsHeaders,
        body: JSON.stringify({ 
          error: `File too large. Maximum size is ${(maxSize / (1024 * 1024 * 1024))}GB` 
        })
      };
    }
    
    // Generate S3 key with timestamp to avoid conflicts
    const timestamp = new Date().toISOString()
      .replace(/[:.]/g, '-')
      .substring(0, 19); // YYYY-MM-DD-HH-mm-ss
    
    // Sanitize filename for S3
    const safeFilename = filename
      .replace(/[^a-zA-Z0-9.-]/g, '_')
      .replace(/_{2,}/g, '_'); // Replace multiple underscores with single
    
    const s3Key = `raw/${timestamp}_${safeFilename}`;
    
    // Determine content type
    const contentTypes = {
      '.mp3': 'audio/mpeg',
      '.wav': 'audio/wav',
      '.m4a': 'audio/mp4'
    };
    const contentType = contentTypes[fileExt] || 'application/octet-stream';
    
    // Build S3 metadata
    const s3Metadata = {
      'original-filename': filename,
      'upload-source': 'audio-hijack-api',
      'upload-timestamp': new Date().toISOString(),
      'api-key-used': event.headers['x-api-key'] ? 'yes' : 'no'
    };
    
    // Add file size if provided
    if (fileSizeBytes) {
      s3Metadata['file-size-bytes'] = String(fileSizeBytes);
      s3Metadata['file-size-mb'] = String((fileSizeBytes / (1024 * 1024)).toFixed(2));
    }
    
    // Add any custom metadata from request
    if (metadata && typeof metadata === 'object') {
      Object.entries(metadata).forEach(([key, value]) => {
        // S3 metadata keys must be lowercase and can't contain certain characters
        const safeKey = key.toLowerCase().replace(/[^a-z0-9-]/g, '-');
        s3Metadata[`custom-${safeKey}`] = String(value);
      });
    }
    
    // Create pre-signed URL for PUT operation
    const command = new PutObjectCommand({
      Bucket: process.env.S3_BUCKET,
      Key: s3Key,
      ContentType: contentType,
      Metadata: s3Metadata,
      // Add tags for easier filtering
      Tagging: 'source=audio-hijack&type=hebrew-audio'
    });
    
    const uploadUrl = await getSignedUrl(s3Client, command, { 
      expiresIn: 3600 // 1 hour
    });
    
    console.log(`Generated pre-signed URL for: ${s3Key}`);
    console.log(`File size: ${fileSizeBytes ? (fileSizeBytes / (1024 * 1024)).toFixed(2) + ' MB' : 'Unknown'}`);
    
    // Return success response
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({
        uploadUrl,
        s3Key,
        bucket: process.env.S3_BUCKET,
        expiresIn: 3600,
        expiresAt: new Date(Date.now() + 3600 * 1000).toISOString(),
        message: 'Upload URL generated successfully',
        fileInfo: {
          originalName: filename,
          size: fileSizeBytes,
          type: contentType
        }
      })
    };
    
  } catch (error) {
    console.error('Error generating pre-signed URL:', error);
    
    // Determine error type for better client handling
    let statusCode = 500;
    let errorMessage = 'Internal server error';
    
    if (error instanceof SyntaxError) {
      statusCode = 400;
      errorMessage = 'Invalid JSON in request body';
    } else if (error.name === 'ValidationError') {
      statusCode = 400;
      errorMessage = error.message;
    }
    
    return {
      statusCode,
      headers: corsHeaders,
      body: JSON.stringify({ 
        error: errorMessage,
        details: process.env.STAGE === 'dev' ? error.message : undefined
      })
    };
  }
};