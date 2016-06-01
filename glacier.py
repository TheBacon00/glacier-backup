# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#Import modules
import os, zipfile, re, boto3, json, base64
from time import sleep
from treehash import TreeHash

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

picture_path = 'D:\\Users\\Garet\\Pictures' #should be arg.
not_archived = []
target_vault_name = 'Photos' #should be arg.

glacier = boto3.client('glacier')

parameters = {
    'Format' : 'JSON',
    'Type' : 'inventory-retrieval',
}

inventory_request = glacier.initiate_job(vaultName=target_vault_name, jobParameters=parameters)

while True:
    response = glacier.describe_job(vaultName=target_vault_name, jobId=inventory_request['jobId'])
    if response['Completed']:
        inventory_data = glacier.get_job_output(vaultName=target_vault_name, jobId=inventory_request['jobId'])        
        break
    print('Job not completed')
    print('Job id: ' + inventory_request['jobId'])
    sleep(600)

data = inventory_data['body'].read()
decoded_data = json.loads(data.decode('utf-8'))

archived = []
decoded_archived = []

for i in decoded_data['ArchiveList']:
    archived.append(i['ArchiveDescription'])
    
for o in archived:
    p1 = re.compile(r'^.+<p>(.+)<\/p>')
    p2 = re.compile(r'^{\"Path\":\"(?P<filename>.+\.\w+)\"')
    m1 = p1.match(o)
    m2 = p2.match(o)
    if m1:
        decode_string = base64.b64decode(m1.group(1)).decode('utf-8')
        p3 = re.compile(r'^(?P<Folder>(?:\w+\/)*)(?P<filename>.+\.\w+)$')
        m3 = p3.match(decode_string)
        if m3:
            decoded_archived.append(m3.group('filename'))
    if m2:
        decoded_archived.append(m2.group('filename'))

#Should be argument to sync files w/ the below match type, or all files?
for entry in os.scandir(picture_path):
    potential_zip = entry.name + '.zip'
    if entry.is_dir() and re.match(r'\d+[_\-]\d+[_\-]\d+', entry.name) and potential_zip not in decoded_archived: 
        not_archived.append({'Zipfile' : potential_zip, 'Path' : (picture_path + '\\' + entry.name)})

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
