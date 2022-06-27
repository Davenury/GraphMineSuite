#!/bin/sh

bucket="gms-us-east-1"
dataset="twitter"

directory=$(dirname $0)

(cd ../GMS && zip -r - *) >GMS.zip

echo $directory
aws s3 mb s3://$bucket
aws s3 cp $directory/../GMS s3://$bucket/GMS --recursive
aws s3 cp ./GMS.zip s3://$bucket/
aws s3 cp $directory/../twitter s3://$bucket/$dataset --recursive
aws s3 cp $directory/bootstrap.sh s3://$bucket/

rm -f ./GMS.zip

echo $bucket
cluster_id=$(aws emr create-cluster --applications Name=Spark Name=Zeppelin --name 'GMS cluster' --ec2-attributes '{"KeyName":"vockey"}' --use-default-roles --enable-debugging --log-uri "s3n://$bucket/elasticmapreduce/" --release-label emr-5.35.0 --instance-type m5.xlarge --instance-count 3 --bootstrap-actions '[{"Path":"'"s3://$bucket/bootstrap.sh"'","Args":["'$bucket'"],"Name":"Bootstrap action"}]' | grep "ClusterId" | awk -F: '{print substr($2,3,length($2)-4)}')

echo $cluster_id
aws emr add-steps --cluster-id $cluster_id --steps Type=spark,Name=GraphProcessing,Args=[--master,yarn,--py-files,s3://$bucket/GMS.zip,--conf,spark.yarn.submit.waitAppCompletion=true,--num-executors,3,--executor-cores,3,--packages,graphframes:graphframes:0.6.0-spark2.3-s_2.11,--executor-memory,10g,s3://$bucket/GMS/aws_run.py,"s3://$bucket/$dataset","$bucket"]
