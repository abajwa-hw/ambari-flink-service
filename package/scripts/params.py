#!/usr/bin/env python
from resource_management import *
from resource_management.libraries.script.script import Script
import sys, os, glob
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.default import default


    
# server configurations
config = Script.get_config()

 
    
# params from flink-ambari-config
flink_install_dir = config['configurations']['flink-ambari-config']['flink_install_dir']
flink_numcontainers = config['configurations']['flink-ambari-config']['flink_numcontainers']
flink_numberoftaskslots= config['configurations']['flink-ambari-config']['flink_numberoftaskslots']
flink_jobmanager_memory = config['configurations']['flink-ambari-config']['flink_jobmanager_memory']
flink_container_memory = config['configurations']['flink-ambari-config']['flink_container_memory']
setup_prebuilt = config['configurations']['flink-ambari-config']['setup_prebuilt']
flink_appname = config['configurations']['flink-ambari-config']['flink_appname']
flink_queue = config['configurations']['flink-ambari-config']['flink_queue']
flink_streaming = config['configurations']['flink-ambari-config']['flink_streaming']

hadoop_conf_dir = config['configurations']['flink-ambari-config']['hadoop_conf_dir']
flink_download_url = config['configurations']['flink-ambari-config']['flink_download_url']
 

conf_dir=''
bin_dir=''

# params from flink-conf.yaml
flink_yaml_content = config['configurations']['flink-env']['content']
flink_user = config['configurations']['flink-env']['flink_user']
flink_group = config['configurations']['flink-env']['flink_group']
flink_log_dir = config['configurations']['flink-env']['flink_log_dir']
flink_log_file = os.path.join(flink_log_dir,'flink-setup.log')



temp_file='/tmp/flink.tgz'
