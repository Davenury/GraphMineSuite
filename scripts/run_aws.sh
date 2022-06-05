#!/bin/sh

bucket="gms-us-east-1"

directory=$(dirname $0)
echo $directory
aws s3 mb s3://$bucket
aws s3 cp $directory/../GMS s3://$bucket/GMS --recursive
# aws s3 cp $directory/../twitter s3://$bucket/twitter --recursive
aws s3 cp $directory/bootstrap.sh s3://$bucket/

echo $bucket
cluster_id=$(aws emr create-cluster --applications Name=Spark Name=Zeppelin --name 'GMS cluster' --ec2-attributes '{"KeyName":"vockey"}' --use-default-roles --enable-debugging --log-uri "s3n://$bucket/elasticmapreduce/" --release-label emr-5.35.0 --instance-type m5.xlarge --instance-count 3 --bootstrap-actions '[{"Path":"'"s3://$bucket/bootstrap.sh"'","Args":["example"],"Name":"Bootstrap action"}]' | grep "ClusterId" | awk --field-separator=":" '{print substr($2,3,length($2)-4)}')

echo $cluster_id
aws emr add-steps --cluster-id $cluster_id --steps Type=spark,Name=GraphProcessing,Args=[--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,--num-executors,3,--executor-cores,3,--executor-memory,10g,s3://$bucket/GMS/aws_run.py,s3://$bucket/]
