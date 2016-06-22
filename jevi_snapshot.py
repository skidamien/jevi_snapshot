#!/usr/bin/python

import boto3  
import datetime
import sys

#def changeme( mylist ):

def getdatetimeObj(aws_backuplasttime):
    mydate = aws_backuplasttime.split(' ', 1 )[0]
    mytime = aws_backuplasttime.split(' ', 1 )[1]

    datearray = mydate.split('-', 2)
    timearray = mytime.split(':', 3)

    month = int(datearray[0])
    day = int(datearray[1])
    year = int(datearray[2])

    hour = int(timearray[0])
    minute = int(timearray[1])
    second = int(timearray[2])
    return datetime.datetime(year, month, day, hour, minute, second)

def addLeftZero(number):
    return "0%s" % number
    
def getBackupLastTimeString():
    mydatetime = datetime.datetime.now()
    minute = mydatetime.minute
    second = mydatetime.second
    if minute < 10:
        minute = addLeftZero(minute)
    if second < 10:
        second = addLeftZero(second)
    return "%s-%s-%s %s:%s:%s" % (mydatetime.month, mydatetime.day, mydatetime.year, mydatetime.hour, minute, second)
    
def verifyTag(tagname, tagvalue):
    varified = False
    if tagname == "BackupFrequencyInDays" or tagname == "BackupRetentionDays":
        if type(tagvalue) is int:
            varified = True
        else:
            varified = False
    elif tagname == "BackupLastTime":
        #Try to split the BackupLastTime string and try to create a datetime object with it
        #if it fails, the string does not have the correct values to make datetime object
        #and thus not a value date / time pair
        print "TAGVAL: %s" %  tagvalue
        try:
            tagvalue_split = tagvalue.split(" ")
            date_split = tagvalue_split[0].split("-")
            time_split = tagvalue_split[1].split(":")
            month = int(date_split[0])
            day = int(date_split[1])
            year = int(date_split[2])
            hour = int(time_split[0])
            minute = int(time_split[1])
            second = int(time_split[2])
            datetime.datetime(year, month, day, hour, minute, second)
            varified = True
        except:
            varified = False
    return varified


ec2 = boto3.client('ec2', region_name='us-east-1')  
ec2_resource = boto3.resource('ec2', region_name='us-east-1')
owner = '368558344414'

#filters = [{'Name': 'tag:Backup', 'Values': ['YUP'] }]

filters = [{  
    'Name': 'tag:Backup',
    'Values': ['Yes', 'yes', 'YES', 'Y', 'y']
    }]

reservations = ec2.describe_instances(Filters=filters)

for reservation in reservations['Reservations']:
    for instance in reservation['Instances']:
        print "===================================================="
        instance_id = instance['InstanceId']
        #print instance['Tags']
        Backup = None
        BackupFrequencyInDays = None
        BackupLastTime = None
        BackupRetentionDays = None
        Name = None

        for tag in instance['Tags']:
            tag_key = tag['Key']
            tag_value = tag['Value']

            if tag_key == "Backup":
                Backup = tag_value.strip()
            
            if tag_key == "BackupFrequencyInDays":
                BackupFrequencyInDays = int(tag_value.strip())
        
            if tag_key == "BackupLastTime":
                BackupLastTime = tag_value.strip()
                
            if tag_key == "BackupRetentionDays":
                BackupRetentionDays = int(tag_value.strip())

            if tag_key == "Name":
                Name = tag_value.strip()
        #
        if BackupFrequencyInDays == None or BackupRetentionDays == None or BackupFrequencyInDays == "" or BackupRetentionDays == "":
            print "passing on this instance, both BackupFrequencyInDays and BackupRetentionDays need a value"
            continue

        errmsg = ""
        varified = True
        if verifyTag('BackupFrequencyInDays', BackupFrequencyInDays) == False:
            errmsg += "\nBackupFrequencyInDays == %s, was detected to be in the wrong format"
            varified = False

        if verifyTag('BackupRetentionDays', BackupRetentionDays) == False:
            errmsg += "\nBackupRetentionDays == %s, was detected to be in the wrong format"
            varified = False
        
        #if no BackupLastTime is set, this is probably the first time it running,
        #lets set it to some really ancient date so we get a first backup
        if BackupLastTime == None or BackupLastTime == "":
            myBackupLastTimeDateTime = datetime.datetime.now()
        else:
            print "BackupLastTime = %s" % BackupLastTime
            if verifyTag('BackupLastTime', BackupLastTime) == False:
                errmsg += "\nBackupLastTime == %s, was detected to be in the wrong format"
                varified = False
            else:
                myBackupLastTimeDateTime = getdatetimeObj(BackupLastTime)

        if varified == False:
            print errmsg
            continue 

        # if the time now is greater than 1 day (ok, 23 hours) after the BackupLastTime timestamp, lets backup the EBS volumes of the instance
        # if not, let's pass this instance and go to the next one       
        BackupFrequencyInHours = (int(BackupFrequencyInDays) * 24) - 1

        print "BackupFrequencyInHours == %d" % BackupFrequencyInHours

        if datetime.datetime.now() > (myBackupLastTimeDateTime + datetime.timedelta(hours=BackupFrequencyInHours)):
            print "The time now is greater than the sum of BackupLastTimeDateTime + %s hours, lets backup the EBS volumes on this instance" % BackupFrequencyInHours       
        else:
            print "The time now is less than the sum of BackupLastTimeDateTime + %s hours, lets pass on this instance" % BackupFrequencyInHours
            continue

        for device in instance['BlockDeviceMappings']:
            device_name = device['DeviceName']
            volume_id = device['Ebs']['VolumeId']
            description = "Jibo Backup: %s %s %s" %  (Name, device_name, volume_id)

            filters = [
                {'Name': 'volume-id', 'Values': [volume_id]}
                ]


            snapshots = ec2.describe_snapshots(Filters=filters,OwnerIds=[owner])
            snapshot_list = snapshots['Snapshots']
            
            snap = ec2.create_snapshot(VolumeId=volume_id,Description=description)
            print "Just created snap: %s" % description
            ec2_resource.create_tags(Resources=[instance_id], Tags=[{'Key': 'BackupLastTime', 'Value': getBackupLastTimeString()}])

            print instance_id
            print "----- "

            # Delete Snapshots that are old
            # Loop through the snapshots in the snapshot list
            # if it is old then delete it
            for snapshot in snapshot_list:
                #print snapshot['Description']
                snapshot_id = snapshot['SnapshotId']
                #print snapshot['StartTime']
                #print type (snapshot['StartTime'])
                
                snapshot_datetime = snapshot['StartTime']

                # if the time now is ahead of the date of the snapshot plus the retention days, its time to delete the snapshot
                # otherwise keep it
                intBackupRetentionDays = int(BackupRetentionDays)
                if datetime.datetime.now().replace(tzinfo=None) > (snapshot_datetime.replace(tzinfo=None) + datetime.timedelta(days=intBackupRetentionDays)):
                    print "Attempting to delete snapshot %s " % snapshot_id 
                    mysnap = ec2_resource.Snapshot(snapshot_id)
                    try:
                        mysnap.delete()
                        print "Successfully deleted snapshot: %s" % snapshot_id
                    except:
                        print "Could not delete snapshot: %s" % snapshot_id
                        print sys.exc_info()[0]
