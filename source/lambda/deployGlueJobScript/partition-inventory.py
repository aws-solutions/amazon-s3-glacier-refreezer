#/*********************************************************************************************************************
# *  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
# *                                                                                                                    *
# *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance    *
# *  with the License. A copy of the License is located at                                                             *
# *                                                                                                                    *
# *      http://www.apache.org/licenses/                                                                               *
# *                                                                                                                    *
# *  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES *
# *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    *
# *  and limitations under the License.                                                                                *
# *********************************************************************************************************************/
#
# @author Solution Builders

import sys
from pyspark.context import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE', 'INVENTORY_TABLE', 'FILENAME_TABLE', 'OUTPUT_TABLE', 'STAGING_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATABASE=args['DATABASE']
INVENTORY_TABLE=args['INVENTORY_TABLE']
FILENAME_TABLE=args['FILENAME_TABLE']
OUTPUT_TABLE=args['OUTPUT_TABLE']
STAGING_BUCKET=args['STAGING_BUCKET']

inventory = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = INVENTORY_TABLE).toDF()
filelist  = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = FILENAME_TABLE)
mapped = filelist.apply_mapping([("archiveid", "string", "archiveid", "string"), ("override", "string", "override", "string")]).toDF().dropDuplicates(['archiveid'])

rownum = inventory.withColumn("row_num", row_number().over(Window.orderBy(inventory['creationdate'],inventory['archiveid'])).cast("long"))
merged = rownum.join(mapped, "archiveid", how='left_outer') 
# merged.show(10)

frame = DynamicFrame.fromDF(merged, glueContext , "merged")

def transform(rec):
  rec["part"] = round(rec["row_num"]/10000)
  rec["archivedescription"] = rec["override"] if rec["override"] and rec["override"].strip() else rec["archivedescription"]
  rec.pop('override', None)
  return rec

trans0 = Map.apply(frame = frame, f = transform)
# trans0.toDF().show(10)

# sink = glueContext.write_dynamic_frame_from_catalog(frame=trans0, 
#                                                     database=DATABASE,
#                                                     table_name=OUTPUT_TABLE, 
#                                                     transformation_ctx="write_sink",
#                                                     additional_options={"enableUpdateCatalog": True, "partitionKeys": ["part"]})

sink = glueContext.getSink(connection_type="s3", path='s3://'+STAGING_BUCKET+'/partitioned/', enableUpdateCatalog=True, partitionKeys=["part"])
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=OUTPUT_TABLE)
sink.writeFrame(trans0)

job.commit()