const { EC2Client, RunInstancesCommand, DescribeInstancesCommand, RequestSpotInstancesCommand, DescribeSpotInstanceRequestsCommand, DescribeVpcsCommand, DescribeSubnetsCommand, CreateTagsCommand, CancelSpotInstanceRequestsCommand, DescribeSecurityGroupsCommand } = require('@aws-sdk/client-ec2');
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
  },
  {
    name: 'us-west-2',
    amiId: process.env.AMI_ID_US_WEST_2,
  },
  {
    name: 'ap-southeast-2',
    amiId: process.env.AMI_ID_AP_SOUTHEAST_2,
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
 * Get Security Group ID by name
 */
async function getSecurityGroupId(ec2Client, stage) {
    const groupName = `hebrew-transcription-${stage}-${await ec2Client.config.region()}`;
    try {
        const result = await ec2Client.send(new DescribeSecurityGroupsCommand({
            GroupNames: [groupName]
        }));
        if (result.SecurityGroups && result.SecurityGroups.length > 0) {
            return result.SecurityGroups[0].GroupId;
        }
        return null;
    } catch (error) {
        console.error(`Could not find security group ${groupName}:`, error);
        return null;
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
  const spotAttempts = [];
  const onDemandAttempts = [];
  
  // Phase 1: Build list of all valid spot attempts across all regions
  console.log('Phase 1: Collecting spot instance options across all regions...');
  for (const region of REGIONS) {
    if (!region.amiId || region.amiId === 'ami-placeholder') {
      console.log(`Skipping ${region.name} - AMI not configured`);
      continue;
    }
    
    const ec2Client = new EC2Client({ region: region.name });
    
    // Get security group for this region
    const securityGroupId = await getSecurityGroupId(ec2Client, process.env.STAGE);
    if (!securityGroupId) {
        console.error(`Could not find security group in ${region.name}`);
        continue;
    }

    // Get subnets for this region
    const subnets = await getDefaultSubnets(ec2Client);
    if (subnets.length === 0) {
      console.error(`No subnets available in ${region.name}`);
      continue;
    }
    
    // Add all subnet options for this region
    for (const subnet of subnets) {
      spotAttempts.push({
        region: region.name,
        amiId: region.amiId,
        ec2Client,
        securityGroupId,
        subnet
      });
      
      onDemandAttempts.push({
        region: region.name,
        amiId: region.amiId,
        ec2Client,
        securityGroupId,
        subnet
      });
    }
  }
  
  // Phase 2: Try all spot instance options
  console.log(`Phase 2: Trying ${spotAttempts.length} spot instance options...`);
  for (const attempt of spotAttempts) {
    let spotInstanceRequestId;
    try {
      console.log(`Requesting spot instance in ${attempt.region} - ${attempt.subnet.AvailabilityZone}...`);
      
      const spotParams = {
        SpotPrice: '2.00', // Max $2/hour for g5.xlarge spot
        InstanceCount: 1,
        Type: 'one-time',
        LaunchSpecification: {
          ImageId: attempt.amiId,
          InstanceType: process.env.INSTANCE_TYPE,
          KeyName: process.env.KEY_NAME,
          SecurityGroupIds: [attempt.securityGroupId],
          SubnetId: attempt.subnet.SubnetId,
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
      const spotResult = await attempt.ec2Client.send(spotCommand);
      spotInstanceRequestId = spotResult.SpotInstanceRequests[0].SpotInstanceRequestId;
      
      // Wait for spot instance to be fulfilled
      console.log(`Waiting for spot request ${spotInstanceRequestId} to be fulfilled...`);
      const instanceId = await waitForSpotInstance(attempt.ec2Client, spotInstanceRequestId);

      if (instanceId) {
        console.log(`✅ Spot instance launched: ${instanceId} in ${attempt.region}`);
        
        // Tag the instance
        await attempt.ec2Client.send(new CreateTagsCommand({
          Resources: [instanceId],
          Tags: [
            { Key: 'Name', Value: `hebrew-transcription-${filenameHash}` },
            { Key: 'AudioFile', Value: filename },
            { Key: 'Project', Value: 'hebrew-transcription' },
            { Key: 'Type', Value: 'spot' }
          ]
        }));
        
        return {
          instanceId: instanceId,
          region: attempt.region,
          type: 'spot',
          availabilityZone: attempt.subnet.AvailabilityZone
        };
      } else {
        // If instanceId is null, it means the request was not fulfilled in time.
        throw new Error('Spot instance request was not fulfilled in time.');
      }
      
    } catch (spotError) {
      console.log(`Spot instance failed in ${attempt.region}/${attempt.subnet.AvailabilityZone}: ${spotError.message}`);
      lastError = spotError;
      
      if (spotInstanceRequestId) {
        console.log(`Cancelling spot request ${spotInstanceRequestId}`);
        try {
          await attempt.ec2Client.send(new CancelSpotInstanceRequestsCommand({
            SpotInstanceRequestIds: [spotInstanceRequestId]
          }));
        } catch (cancelError) {
          console.error(`Error cancelling spot request: ${cancelError.message}`);
        }
      }
    }
  }
  
  // Phase 3: If all spot attempts failed, try on-demand instances
  console.log(`Phase 3: All spot attempts failed. Trying ${onDemandAttempts.length} on-demand options...`);
  for (const attempt of onDemandAttempts) {
    try {
      console.log(`Trying on-demand instance in ${attempt.region} - ${attempt.subnet.AvailabilityZone}...`);
      
      const onDemandParams = {
        ImageId: attempt.amiId,
        InstanceType: process.env.INSTANCE_TYPE,
        KeyName: process.env.KEY_NAME,
        SecurityGroupIds: [attempt.securityGroupId],
        SubnetId: attempt.subnet.SubnetId,
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
      const onDemandResult = await attempt.ec2Client.send(onDemandCommand);
      const instanceId = onDemandResult.Instances[0].InstanceId;
      
      console.log(`✅ On-demand instance launched: ${instanceId} in ${attempt.region}`);
      
      return {
        instanceId,
        region: attempt.region,
        type: 'on-demand',
        availabilityZone: attempt.subnet.AvailabilityZone
      };
      
    } catch (onDemandError) {
      console.error(`On-demand failed in ${attempt.region}/${attempt.subnet.AvailabilityZone}: ${onDemandError.message}`);
      lastError = onDemandError;
    }
  }
  
  // All attempts failed
  throw lastError || new Error('Failed to launch instance in all regions and types');
}

/**
 * Polls for spot instance fulfillment
 */
async function waitForSpotInstance(ec2Client, spotInstanceRequestId) {
  // Monitor spot request (wait for fulfillment or timeout)
  const maxWaitTime = 3 * 60 * 1000; // 3 minutes (reduced from 5 to allow on-demand fallback)
  const pollInterval = 10000; // 10 seconds
  const startTime = Date.now();

  while (Date.now() - startTime < maxWaitTime) {
    const checkResult = await ec2Client.send(new DescribeSpotInstanceRequestsCommand({
      SpotInstanceRequestIds: [spotInstanceRequestId]
    }));

    const request = checkResult.SpotInstanceRequests[0];
    console.log(`Spot request ${spotInstanceRequestId} status: ${request.State}`);

    if (request.State === 'active' && request.InstanceId) {
      return request.InstanceId;
    }

    if (['cancelled', 'failed', 'closed'].includes(request.State)) {
      console.error(`Spot request failed with state: ${request.State}. Reason: ${request.Status.Message}`);
      return null;
    }
    
    await new Promise(resolve => setTimeout(resolve, pollInterval));
  }
  
  console.error(`Spot request ${spotInstanceRequestId} timed out after 3 minutes.`);
  return null;
}

/**
 * Update DynamoDB with instance launch status
 */
async function updateDynamoDBStatus(id, status, instanceInfo = null) {
  const timestamp = new Date().toISOString();
  
  const params = {
    TableName: process.env.DYNAMODB_TABLE,
    Key: { id },
    UpdateExpression: 'SET #status = :status, #updated = :updated',
    ExpressionAttributeNames: {
      '#status': 'status',
      '#updated': 'lastUpdated'
    },
    ExpressionAttributeValues: {
      ':status': status,
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
  
  const { action, s3Key, s3Bucket, validation, id } = event;
  
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
      await updateDynamoDBStatus(id, 'processing', instanceInfo);
      
      return { ...instanceInfo, id };
    } else {
      throw new Error(`Unknown action: ${action}`);
    }
    
  } catch (error) {
    console.error('Error:', error);
    
    // Update DynamoDB with error status
    if (id) {
      await updateDynamoDBStatus(id, 'error', {
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
    
    throw error;
  }
};