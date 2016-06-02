# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#Import modules
import os, zipfile, re, boto3, json, base64, logging, getopt, sys
from datetime import datetime
from time import sleep
from treehash import TreeHash

#Set the arguments
optlist, remainder = getopt.getopt(sys.argv[1:], 'v:p:', ['vault=','path='])

picture_path = 'D:\\Users\\Garet\\Pictures' #should be arg.
target_vault_name = 'Photos' #should be arg.

for opt, arg in optlist:
    if opt == '--vault':
        target_vault_name = arg
    if opt == '--path':
        picture_path = arg

#Add logging
logger = logging.getLogger('test_app')

logger.setLevel(logging.DEBUG)

now = datetime.now().strftime('%d%m%Y-%H%M%S')

fh = logging.FileHandler('log-{timestamp}.txt'.format(timestamp=now))
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

#Functions
def writeZipfile(path, zipfile_object, archive_path=''):
    for f in os.scandir(path):
        if f.is_file():
            if archive_path == '':
                zipfile_object.write(f.path, arcname=f.name)
            else:
                zipfile_object.write(f.path, arcname=archive_path + '\\' + f.name)
        elif f.is_dir():
            if archive_path == '':
                writeZipfile(f.path, zipfile_object, archive_path=f.name)
            else:
                writeZipfile(f.path, zipfile_object, archive_path=(archive_path + '\\' + f.name))

def getExistingUploads(glacier_client, vault_name, previous_result=None, page_marker=None):
    all_uploads = []
    if page_marker == None:
        uploads = glacier_client.list_multipart_uploads(vaultName=vault_name)
    else:
        uploads = glacier_client.list_multipart_uploads(vaultName=vault_name, marker=page_marker)
    
    if previous_result != None:
        all_uploads.append(previous_result)
    for upload in uploads['UploadsList']:
        all_uploads.append(upload)
    
    if 'marker' in uploads.keys():
        getExistingUploads(glacier_client, previous_result=all_uploads, page_marker=uploads['marker'])
    else:
        return all_uploads
        
def getExistingParts(glacier_client, vault_name, upload_id, previous_result=None, page_marker=None):
    all_parts = []
    parts = glacier_client.list_parts(vaultName=vault_name, uploadId=upload_id)
    
    if previous_result != None:
        all_parts.append(previous_result)
    for part in parts['Parts']:
        all_parts.append(part)
    
    if 'marker' in parts.keys():
        getExistingParts(glacier_client, vault_name, upload_id, previous_result=all_parts, page_marker=parts['Marker'])
    else:
        return {'Parts': all_parts, 'PartSizeInBytes': parts['PartSizeInBytes']}

def startUpload(glacier_client, vault_name, archive_description, chunk_size):
    result = glacier_client.initiate_multipart_upload(
        vaultName=vault_name,
        archiveDescription=archive_description,
        partSize=chunk_size
    )
    return result

def uploadPart(glacier_client, vault_name, upload_id, start_at, end_at, chunk, chunk_hash):   
        glacier_client.upload_multipart_part(
            vaultName=vault_name,
            uploadId=upload_id,
            range='bytes ' + start_at + '-' + end_at + '/*',
            checksum=chunk_hash,
            body=chunk
        )

not_archived = []
logger.debug('Set not_archived to an empty list')

glacier = boto3.client('glacier')
logger.debug('Set glacier to boto3 client.')

parameters = {
    'Format' : 'JSON',
    'Type' : 'inventory-retrieval',
}
logger.debug('Glacier inventory request parameters set to: ' + str(parameters))

try:
    inventory_request = glacier.initiate_job(vaultName=target_vault_name, jobParameters=parameters)
except:
    logger.critical('Failure getting inventory request from Glacier. Please re-run the program. Exiting with ConnectionError.')
    raise ConnectionError('Could not connect to Glacier for inventory export.')

logger.info('Inventory request response: ' + str(inventory_request))
logger.info('Entering while loop to check for completed inventory request.')

while True:
    try:
        logger.info('Querying Glacier for description of current jobs.')
        response = glacier.describe_job(vaultName=target_vault_name, jobId=inventory_request['jobId'])
        logger.debug('Response from Glacier concerning jobs: ' + str(response))
    except:
        logger.error('Problem connecting to Glacier, will retry after wait period.')
    if response['Completed']:
        try:
            logger.info('Job completed!')
            inventory_data = glacier.get_job_output(vaultName=target_vault_name, jobId=inventory_request['jobId'])
            logger.debug('inventory_data: ' + str(inventory_data))
        except:
            logger.critical('Failure getting job output from Glacier!')
            raise ConnectionError('Could not get job output from Glacier.')
        break
    logger.info('Job not complete')
    logger.debug('Job id: ' + inventory_request['jobId'])
    logger.info('Entering sleep state...')
    sleep(600)
    logger.debug('Sleep done, restarting loop.')


try:
    logger.info('Reading inventory data and converting from JSON to dict object.')
    data = inventory_data['body'].read()
    decoded_data = json.loads(data.decode('utf-8'))
except:
    logger.critical('Problem converting data from JSON to dict object.')
    raise RuntimeError('Problem converting JSON data to dict object.')

archived = []
decoded_archived = []

logger.debug('Adding all ArchiveDescriptions from current Glacier inventory to [archived] list.')
for i in decoded_data['ArchiveList']:
    archived.append(i['ArchiveDescription'])

logger.debug('Starting regex matching of ArchiveDescriptions.')
for o in archived:
    logger.info('Checking ' + str(o))
    p1 = re.compile(r'^.+<p>(.+)<\/p>')
    p2 = re.compile(r'^{\"Path\":\"(?P<filename>.+\.\w+)\"')
    m1 = p1.match(o)
    m2 = p2.match(o)
    if m1:
        logger.debug(str(o) + ' is a match, doing Base64 decoding to determine actual ArchiveDescription.')
        decode_string = base64.b64decode(m1.group(1)).decode('utf-8')
        logger.info('Decoded to ' + str(decode_string))
        p3 = re.compile(r'^(?P<Folder>(?:\w+\/)*)(?P<filename>.+\.\w+)$')
        m3 = p3.match(decode_string)
        if m3:
            logger.info(str(decode_string) + ' is a match!')
            decoded_archived.append(m3.group('filename'))
        else:
            logger.debug(str(decode_string) + ' is not a match.')
    else:
        logger.debug(str(o) + ' is not a match.')
    if m2:
        logger.info(str(o) + ' is a match!')
        decoded_archived.append(m2.group('filename'))
    else:
        logger.debug(str(o) + ' is not a match.')

#Should be argument to sync files w/ the below match type, or all files?
logger.info('Checking Glacier inventory against what is at ' + str(picture_path))
for entry in os.scandir(picture_path):
    logger.debug('Found ' + entry.name + ' at ' + picture_path )
    potential_zip = entry.name + '.zip'
    if entry.is_dir() and re.match(r'\d+[_\-]\d+[_\-]\d+', entry.name) and potential_zip not in decoded_archived: 
        logger.info(entry.name + ' matches the pattern \d+[_\-]\d+[_\-]\d+ and is not currently in Glacier as a zip file.')        
        not_archived.append({'Zipfile' : potential_zip, 'Path' : (picture_path + '\\' + entry.name)})
        logger.info(potential_zip + ' added to list of archives that need to be created uploaded.')

for n in not_archived:
    with zipfile.ZipFile(n['Zipfile'], 'w', allowZip64=True) as zf:
        writeZipfile(n['Path'], zf)
    
    with open(n['Zipfile'], 'rb') as file:
        expected_fullhash = TreeHash()
        expected_fullhash.update(file.read())
        expected_fullhash_value = expected_fullhash.hexdigest()
        file.seek(0)
        #Check for multipart upload in progress
        existing_uploads = getExistingUploads(glacier, target_vault_name)
    
        start_byte = 0
        file_size = 0
        chunksize = 8388608
        fullhash = TreeHash()
        description = '{"Path":"' + n['Zipfile'] + ', "ExpectedTreeHash":"' + expected_fullhash_value + '"}'
    
        matching_uploads = list(filter(lambda x: x['ArchiveDescription'] == description, existing_uploads))
        
        if len(matching_uploads) > 0:
            print('Existing upload, resuming...')
            upload = matching_uploads[-1]
            existing_parts = getExistingParts(glacier, target_vault_name, upload['MultipartUploadId'])
            if len(existing_parts['Parts']) > 0:
                last_part = existing_parts['Parts'][-1]
                start_byte = int(last_part['RangeInBytes'].split('-')[1]) + 1
            session_uploadId = upload['MultipartUploadId']     
            chunksize = upload['PartSizeInBytes']
            print('Start byte will be', start_byte)
    
        else:
            print('New upload!')
            new_upload = startUpload(
                glacier_client=glacier,
                vault_name=target_vault_name,
                archive_description=description,
                chunk_size = str(chunksize)
            )
            session_uploadId = new_upload['uploadId']
                    
        end_byte = (-1)
        if start_byte != 0:
            file_size = file_size + (start_byte)
            uploaded_bytes = file.read(start_byte)
            fullhash.update(uploaded_bytes)
            uploaded_bytes = None
            end_byte = start_byte - 1
        
        while True:
            chunk_bytes = file.read(chunksize)
            if not chunk_bytes:
                break
            chunk_length = len(chunk_bytes)
            file_size = file_size + len(chunk_bytes)
            
            chunkhash = TreeHash()
            
            fullhash.update(chunk_bytes)
            chunkhash.update(chunk_bytes)
            
            chunk_hash_value = chunkhash.hexdigest()
            
            end_byte = end_byte + chunk_length 
            
            print('start byte', start_byte)
            print('end_byte', end_byte)
            
            uploadPart(
                glacier_client=glacier,
                vault_name=target_vault_name,
                upload_id = session_uploadId, 
                chunk_hash = chunk_hash_value,
                start_at=str(start_byte),
                end_at=str(end_byte),
                chunk=chunk_bytes
            )
            
            start_byte = end_byte + 1
            
    full_hash_value = fullhash.hexdigest()
    
    try:
        print('Upload completed for ' + n['Zipfile'])
        completion_results = glacier.complete_multipart_upload(
            vaultName=target_vault_name,
            uploadId=session_uploadId,
            archiveSize=str(file_size),
            checksum=str(full_hash_value)
        )
    except:
        print('Could not complete upload. Possibly TreeHashes did not match. Deleting MultipartUpload {uploadId} for manual re-try.'.format(uploadId=session_uploadId)) 
        glacier.abort_multipart_upload(uploadId=session_uploadId, vaultName=target_vault_name)
              

    #If there is one and it matches n, resume it from the last chunk uploaded.
    
    #Delete n
    os.remove(n['Zipfile'])
