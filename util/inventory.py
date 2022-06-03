import os
import csv
import json
import gzip
import time
import boto3
import logging
from typing import Dict
from base64 import decode
from datetime import date
from botocore.exceptions import ClientError

#logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


PARENT_DIR = os.getcwd()
ORIGINAL_DIR = os.path.join(PARENT_DIR,"penneo-docs-inventory")
INVENTORY_DIR = os.path.join(PARENT_DIR, "inventory")
UNIQ_DIR = os.path.join(INVENTORY_DIR, "uniqs")

class Inventory:
    
    def __init__(self, inventory_bucket, target_bucket, aws_profile=None):
        self.inventory_bucket = inventory_bucket
        self.target_bucket = target_bucket
        self.aws_profile = aws_profile

        #Match objects
        self.count = 0
        self.availables = set()

        #client
        if self.aws_profile is not None:
            self.session = boto3.session.Session(profile_name=self.aws_profile)
            try:
                self.s3 = self.session.client('s3')
            except ClientError:
                raise
        else:
            try:
                self.s3 = boto3.client('s3')
            except ClientError:
                raise

    def has_tags(self, obj_key: str, tags: Dict[str, str]) -> bool:
        """
        Check if object keys has all the specified tags
        """

        try:
            response = self.s3.get_object_tagging(
                Bucket=self.target_bucket,
                Key=obj_key
            )
        except ClientError as err:
            raise
        except Exception as err:
            raise
        for k, v in tags.items():
            match = 0
            for tag in response['TagSet']:
                if k == tag['Key'] and v == tag['Value']:
                    match = 1
            if match == 0:
                return False
        return True     
    
    def _list_objects(self):
        try:
            response = self.s3.list_objects(
                Bucket=self.inventory_bucket
            )
        except Exception as err:
            raise err
        return response

    def available(self):
        """
        This will display all available inventories in the inventory bucket
        """
        result = self._list_objects()
        #availables = set()
        for avail in result['Contents']:
            if 'data' not in avail['Key'] and 'hive' not in avail['Key']: 
                self.availables.add('/'.join(avail['Key'].split('/')[:-1]))
        #today = date.today()
        #today = today.strftime('"%y-%m-%d"')
        return self.availables
    
    def get_info(self, inventory: str):
        """
        get will get the specific manifest
        """
        #if available is empty might means you need to get available inventory objects
        if len(self.availables) <= 0:
            self.available()
            #but if still empty no available inventories in the bucket
            if len(self.availables) <= 0:
                raise Exception('Inventory bucket {} is empty'.format(self.inventory_bucket))       

        if inventory not in self.availables:
            raise Exception("Unable to find inventory: {}".format(inventory))
        
        result = self._list_objects()
        for obj in result['Contents']:
            if inventory in obj['Key'] and 'manifest.json' in obj['Key']:
                with open('manifest.json', 'wb') as data:
                    self.s3.download_fileobj(self.inventory_bucket, obj['Key'], data)
        
        manifest = open('manifest.json')
        data = json.load(manifest)
        #os.remove('manifest.json') #not sure if best thing is to delete, I'll decide later on
        return data

    def download(self, inventory: str):
        """
        Download the inventory files in the manifest.json
        """
        try:
            manifest = self.get_info(inventory)
        except Exception as error:
            raise
        
        #create the inventory file directory
        inventory_dir = '-'.join(inventory.split('/'))
        os.mkdir(inventory_dir)

        if self.aws_profile is not None:
            s3 = self.session.resource('s3')
        else:
            s3 = boto3.resource('s3')

        #Download
        for file in manifest['files']:
            try:
                s3.meta.client.download_file(
                    self.inventory_bucket, 
                    file['key'], 
                    os.path.join(inventory_dir,
                    file['key'].split('/')[-1]))
            except Exception as error:
                raise
        

def cleaning():
    print("Start cleaning...")
    time.sleep(1)
    os.system("rm -rf {}".format(INVENTORY_DIR))
    os.system("cp -r {} {}".format(ORIGINAL_DIR, INVENTORY_DIR))
    time.sleep(1)
    print("Done!")
    return

def gunzip(_dir: str):
    os.chdir(_dir)
    for item in os.listdir(os.getcwd()):  # loop through items in dir
        if item.endswith('gz'):
            with open(item, 'rb') as inf, open('.'.join(item.split('.')[0:2]),'w', encoding='utf8') as outf:   
                decomp = gzip.decompress(inf.read()).decode('utf-8')
                outf.write(decomp)
            os.remove(item)
    return


def generate_unique_object_files(_dir):

    print("Generatig directory for the files with the unique objects")
    os.chdir(_dir)
    for item in os.listdir():
        if item.endswith('csv'):         
            with open(item, "r") as inf, open(os.path.join(UNIQ_DIR, item), "w") as outf:
                reader = csv.reader(inf)
                writer = csv.writer(outf)
                seen = set()
                for row in reader:    
                    if row[1] in seen:
                        continue
                    seen.add(row[1])
                    writer.writerow(row[1].split(','))
    return


if __name__ == "__main__":
    

    
    os.chdir(UNIQ_DIR)
    count = 0
    for item in os.listdir():
        print("Scaning file... {}".format(item))
        with open(item, 'r') as inf:
            for obj_key in inf:
                if has_tags(s3, 'penneo-documents',obj_key.rstrip('\n'), TAGS):
                    print("match, current count: {}".format(count))
                    count += 1
            print("This file contains: {} matching the looking tag".format(count))
    print(count)
