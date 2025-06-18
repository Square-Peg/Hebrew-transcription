const { EC2Client, RunInstancesCommand, DescribeInstancesCommand, RequestSpotInstancesCommand, DescribeSpotInstanceRequestsCommand, DescribeVpcsCommand, DescribeSubnetsCommand, CreateTagsCommand } = require('@aws-sdk/client-ec2');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const crypto = require('crypto');

// Initialize clients
const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

// Regional configuration with failover order
const REGIONS = [
  {
    name: 'us-east-1',
    amiId: process.env.AMI_ID_US_EAST_1,
    securityGroupId: process.env.SECURITY_GROUP_ID_US_EAST_1,
  },
  {
    name: 'us-west-2',
    amiId: process.env.AMI_ID_US_WEST_2,
    securityGroupId: process.env.SECURITY_GROUP_ID_US_WEST_2,
  },
  {
    name: 'ap-southeast-2',
    amiId: process.env.AMI_ID_AP_SOUTHEAST_2,
    securityGroupId: process.env.SECURITY_GROUP_ID_AP_SOUTHEAST_2,
  }
];

/**
 * Check for existing running instances
 */
async function checkForRunningInstances() {
  console.log('Checking for existing running instances across all regions...');
  
  for (const region of REGIONS) {
    const ec2Client = new EC2Client({ region: region.name });
    
    try {
      const command = new DescribeInstancesCommand({
        Filters: [
          {
            Name: 'tag:Project',
            Values: ['hebrew-transcription']
          },
          {
            Name: 'instance-state-name',
            Values: ['running', 'pending']
          }
        ]
      });
      
      const result = await ec2Client.send(command);
      const instances = result.Reservations?.flatMap(r => r.Instances) || [];
      
      if (instances.length > 0) {
        console.log(`Found ${instances.length} running instance(s) in ${region.name}`);
        return {
          hasRunning: true,
          region: region.name,
          instances: instances.map(i => ({
            instanceId: i.InstanceId,
            launchTime: i.LaunchTime,
            audioFile: i.Tags?.find(t => t.Key === 'AudioFile')?.Value
          }))
        };
      }
    } catch (error) {
      console.error(`Error checking instances in ${region.name}:`, error);
    }
  }
  
  return { hasRunning: false };
}

/**
 * Get default subnets for a region
 */
async function getDefaultSubnets(ec2Client) {
  try {
    const vpcsResult = await ec2Client.send(new DescribeVpcsCommand({
      Filters: [{ Name: 'isDefault', Values: ['true'] }]
    }));
    
    if (!vpcsResult.Vpcs || vpcsResult.Vpcs.length === 0) {
      throw new Error('No default VPC found');
    }
    
    const defaultVpcId = vpcsResult.Vpcs[0].VpcId;
    
    const subnetsResult = await ec2Client.send(new DescribeSubnetsCommand({
      Filters: [
        { Name: 'vpc-id', Values: [defaultVpcId] },
        { Name: 'default-for-az', Values: ['true'] }
      ]
    }));
    
    return subnetsResult.Subnets || [];
  } catch (error) {
    console.error('Error getting default subnets:', error);
    return [];
  }
}

/**
 * Launch instance with spot/on-demand fallback
 */
async function launchInstance(s3Key, s3Bucket, validation) {
  const filename = s3Key.split('/').pop();
  const filenameHash = crypto.createHash('md5').update(filename).digest('hex').substring(0, 8);
  
  // Create user data script
  const userDataScript = Buffer.from(`#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "[$(date)] Starting Hebrew transcription for ${filename}"

# Set environment variables
export S3_BUCKET="${s3Bucket}"
export S3_KEY="${s3Key}"
export FILENAME="${filename}"

# Run the startup script
/opt/hebrew-transcription/startup.sh

echo "[$(date)] Transcription complete, shutting down"
sudo shutdown -h now
`).toString('base64');

  let lastError;
  
  // Try each region in order
  for (const region of REGIONS) {
    if (!region.amiId || region.amiId === 'ami-placeholder') {
      console.log(`Skipping ${region.name} - AMI not configured`);
      continue;
    }
    
    const ec2Client = new EC2Client({ region: region.name });
    console.log(`Attempting to launch in ${region.name}...`);
    
    // Get subnets for this region
    const subnets = await getDefaultSubnets(ec2Client);
    if (subnets.length === 0) {
      console.error(`No subnets available in ${region.name}`);
      continue;
    }
    
    // Try each subnet
    for (const subnet of subnets) {
      try {
        // First try spot instance
        console.log(`Requesting spot instance in ${subnet.AvailabilityZone}...`);
        
        const spotParams = {
          SpotPrice: '2.00', // Max $2/hour for g5.xlarge spot
          InstanceCount: 1,
          Type: 'one-time',
          LaunchSpecification: {
            ImageId: region.amiId,
            InstanceType: process.env.INSTANCE_TYPE,
            KeyName: process.env.KEY_NAME,
            SecurityGroupIds: [region.securityGroupId],
            SubnetId: subnet.SubnetId,
            IamInstanceProfile: { Name: process.env.IAM_INSTANCE_PROFILE },
            BlockDeviceMappings: [
              {
                DeviceName: '/dev/sda1',
                Ebs: {
                  VolumeSize: 100,
                  VolumeType: 'gp3',
                  DeleteOnTermination: true
                }
              }
            ],
            UserData: userDataScript
          }
        };
        
        const spotCommand = new RequestSpotInstancesCommand(spotParams);
        const spotResult = await ec2Client.send(spotCommand);
        
        // Wait a moment then check if spot request was fulfilled
        await new Promise(resolve => setTimeout(resolve, 5000));
        
        const checkCommand = new DescribeSpotInstanceRequestsCommand({
          SpotInstanceRequestIds: spotResult.SpotInstanceRequests.map(r => r.SpotInstanceRequestId)
        });
        
        const checkResult = await ec2Client.send(checkCommand);
        const request = checkResult.SpotInstanceRequests[0];
        
        if (request.State === 'active' && request.InstanceId) {
          console.log(`✅ Spot instance launched: ${request.InstanceId} in ${region.name}`);
          
          // Tag the instance
          await ec2Client.send(new CreateTagsCommand({
            Resources: [request.InstanceId],
            Tags: [
              { Key: 'Name', Value: `hebrew-transcription-${filenameHash}` },
              { Key: 'AudioFile', Value: filename },
              { Key: 'Project', Value: 'hebrew-transcription' },
              { Key: 'Type', Value: 'spot' }
            ]
          }));
          
          return {
            instanceId: request.InstanceId,
            region: region.name,
            type: 'spot',
            availabilityZone: subnet.AvailabilityZone
          };
        }
        
      } catch (spotError) {
        console.log(`Spot instance failed in ${subnet.AvailabilityZone}: ${spotError.message}`);
        
        // Try on-demand as fallback
        try {
          console.log(`Trying on-demand instance in ${subnet.AvailabilityZone}...`);
          
          const onDemandParams = {
            ImageId: region.amiId,
            InstanceType: process.env.INSTANCE_TYPE,
            KeyName: process.env.KEY_NAME,
            SecurityGroupIds: [region.securityGroupId],
            SubnetId: subnet.SubnetId,
            IamInstanceProfile: { Name: process.env.IAM_INSTANCE_PROFILE },
            MinCount: 1,
            MaxCount: 1,
            InstanceInitiatedShutdownBehavior: 'terminate',
            BlockDeviceMappings: [
              {
                DeviceName: '/dev/sda1',
                Ebs: {
                  VolumeSize: 100,
                  VolumeType: 'gp3',
                  DeleteOnTermination: true
                }
              }
            ],
            UserData: userDataScript,
            TagSpecifications: [
              {
                ResourceType: 'instance',
                Tags: [
                  { Key: 'Name', Value: `hebrew-transcription-${filenameHash}` },
                  { Key: 'AudioFile', Value: filename },
                  { Key: 'Project', Value: 'hebrew-transcription' },
                  { Key: 'Type', Value: 'on-demand' }
                ]
              }
            ]
          };
          
          const onDemandCommand = new RunInstancesCommand(onDemandParams);
          const onDemandResult = await ec2Client.send(onDemandCommand);
          const instanceId = onDemandResult.Instances[0].InstanceId;
          
          console.log(`✅ On-demand instance launched: ${instanceId} in ${region.name}`);
          
          return {
            instanceId,
            region: region.name,
            type: 'on-demand',
            availabilityZone: subnet.AvailabilityZone
          };
          
        } catch (onDemandError) {
          console.error(`On-demand failed in ${subnet.AvailabilityZone}: ${onDemandError.message}`);
          lastError = onDemandError;
        }
      }
    }
  }
  
  // All regions failed
  throw lastError || new Error('Failed to launch instance in all regions');
}

/**
 * Update DynamoDB with instance launch status
 */
async function updateDynamoDBStatus(filename, status, instanceInfo = null) {
  const timestamp = new Date().toISOString();
  
  const params = {
    TableName: process.env.DYNAMODB_TABLE,
    Key: { filename },
    UpdateExpression: 'SET #status = :status, #timestamp = :timestamp, #updated = :updated',
    ExpressionAttributeNames: {
      '#status': 'status',
      '#timestamp': 'timestamp',
      '#updated': 'lastUpdated'
    },
    ExpressionAttributeValues: {
      ':status': status,
      ':timestamp': timestamp,
      ':updated': timestamp
    }
  };
  
  if (instanceInfo) {
    params.UpdateExpression += ', #instance = :instance';
    params.ExpressionAttributeNames['#instance'] = 'instanceInfo';
    params.ExpressionAttributeValues[':instance'] = instanceInfo;
  }
  
  await docClient.send(new UpdateCommand(params));
}

/**
 * Main handler
 */
exports.handler = async (event) => {
  console.log('Event:', JSON.stringify(event, null, 2));
  
  const { action, s3Key, s3Bucket, validation } = event;
  
  try {
    if (action === 'check') {
      // Check for existing instances
      const result = await checkForRunningInstances();
      return result;
      
    } else if (action === 'launch') {
      // First check again for running instances (race condition prevention)
      const instanceCheck = await checkForRunningInstances();
      if (instanceCheck.hasRunning) {
        console.log('Instance already running, skipping launch');
        return {
          ...instanceCheck,
          skipped: true
        };
      }
      
      // Launch new instance
      const instanceInfo = await launchInstance(s3Key, s3Bucket, validation);
      
      // Update DynamoDB
      const filename = s3Key.split('/').pop();
      await updateDynamoDBStatus(filename, 'processing', instanceInfo);
      
      return instanceInfo;
    } else {
      throw new Error(`Unknown action: ${action}`);
    }
    
  } catch (error) {
    console.error('Error:', error);
    
    // Update DynamoDB with error status
    if (s3Key) {
      const filename = s3Key.split('/').pop();
      await updateDynamoDBStatus(filename, 'error', {
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
    
    throw error;
  }
};