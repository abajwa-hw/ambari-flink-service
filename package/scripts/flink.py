import sys, os, pwd, grp, signal, time, glob, subprocess
from resource_management import *
from subprocess import call





class Master(Script):
  def install(self, env):

    import params
    import status_params
    user_home = "/home/" + str(params.flink_user)
    try:
      pwd.getpwnam(params.flink_user)
    except KeyError:
      User(params.flink_user,
           home=user_home,
           shell="/bin/bash",
           ignore_failures=True)
            
    #e.g. /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/FLINK/package
    service_packagedir = os.path.realpath(__file__).split('/scripts')[0] 
            
    Execute('rm -rf ' + params.flink_install_dir, ignore_failures=True)
            
    Directory([status_params.flink_pid_dir, params.flink_log_dir, params.flink_install_dir],
            owner=params.flink_user,
            group=params.flink_group
    )   

    File(params.flink_log_file,
            mode=0644,
            owner=params.flink_user,
            group=params.flink_group,
            content=''
    )
    

         
    
    #User selected option to use prebuilt flink package 
    if params.setup_prebuilt:

      Execute('echo Installing packages')
        
 
      #Fetch and unzip snapshot build, if no cached flink tar package exists on Ambari server node
      if not os.path.exists(params.temp_file):
        Execute('wget '+params.flink_download_url+' -O '+params.temp_file+' -a '  + params.flink_log_file, user=params.flink_user)
        Execute('tar -zxvf '+params.temp_file+' -C ' + params.flink_install_dir + ' >> ' + params.flink_log_file, user=params.flink_user)
        Execute('mv '+params.flink_install_dir+'/*/* ' + params.flink_install_dir, user=params.flink_user)
        Execute('wget ' + params.flink_hadoop_shaded_jar_url + ' -P ' + params.flink_install_dir+'/lib' + ' >> ' + params.flink_log_file, user=params.flink_user)

      #update the configs specified by user
      self.configure(env, True)

      
    else:
      #User selected option to build flink from source
       
      #if params.setup_view:
        #Install maven repo if needed
      self.install_mvn_repo()      
      # Install packages listed in metainfo.xml
      self.install_packages(env)    
    
      
      Execute('echo Compiling Flink from source')
      Execute('cd '+params.flink_install_dir+'; git clone https://github.com/apache/flink.git '+params.flink_install_dir +' >> ' + params.flink_log_file)
      Execute('chown -R ' + params.flink_user + ':' + params.flink_group + ' ' + params.flink_install_dir)
                
      Execute('cd '+params.flink_install_dir+'; mvn clean install -DskipTests -Dhadoop.version=2.7.1.2.3.2.0-2950 -Pvendor-repos >> ' + params.flink_log_file, user=params.flink_user)
      
      #update the configs specified by user
      self.configure(env, True)

  

  def configure(self, env, isInstall=False):
    import params
    import status_params
    env.set_params(params)
    env.set_params(status_params)
    
    self.set_conf_bin(env)
        
    #write out nifi.properties
    properties_content=InlineTemplate(params.flink_yaml_content)
    File(format("{conf_dir}/flink-conf.yaml"), content=properties_content, owner=params.flink_user)
        
    
  def stop(self, env):
    import params
    import status_params    
    from resource_management.core import sudo
    pid = str(sudo.read_file(status_params.flink_pid_file))
    Execute('yarn application -kill ' + pid, user=params.flink_user)

    Execute('rm ' + status_params.flink_pid_file, ignore_failures=True)
 
      
  def start(self, env):
    import params
    import status_params
    self.set_conf_bin(env)  
    self.configure(env) 
    
    self.create_hdfs_user(params.flink_user)

    Execute('echo bin dir ' + params.bin_dir)        
    Execute('echo pid file ' + status_params.flink_pid_file)
    cmd_open = subprocess.Popen(["hadoop", "classpath"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    hadoop_classpath = cmd_open.communicate()[0].strip()
    cmd = format("export HADOOP_CONF_DIR={hadoop_conf_dir}; export HADOOP_CLASSPATH={hadoop_classpath}; {bin_dir}/yarn-session.sh -d -nm {flink_appname} -n {flink_numcontainers} -s {flink_numberoftaskslots} -jm {flink_jobmanager_memory} -tm {flink_container_memory} -qu {flink_queue}")
    if params.flink_streaming:
      cmd = cmd + ' -st '
    Execute (cmd + format(" >> {flink_log_file}"), user=params.flink_user)
    Execute("yarn application -list 2>/dev/null | awk '/" + params.flink_appname + "/ {print $1}' | head -n1 > " + status_params.flink_pid_file, user=params.flink_user)
    #Execute('chown '+params.flink_user+':'+params.flink_group+' ' + status_params.flink_pid_file)

    if os.path.exists(params.temp_file):
      os.remove(params.temp_file)

  def check_flink_status(self, pid_file):
    from datetime import datetime 
    from resource_management.core.exceptions import ComponentIsNotRunning
    from resource_management.core import sudo
    from subprocess import PIPE,Popen
    import shlex, subprocess
    if not pid_file or not os.path.isfile(pid_file):
      raise ComponentIsNotRunning()
    try:
      pid = str(sudo.read_file(pid_file)) 
      cmd_line = "/usr/bin/yarn application -list"
      args = shlex.split(cmd_line)
      proc = Popen(args, stdout=PIPE)
      p = str(proc.communicate()[0].split())
      if p.find(pid.strip()) < 0:
        raise ComponentIsNotRunning() 
    except Exception, e:
      raise ComponentIsNotRunning()

  def status(self, env):
    import status_params       
    from datetime import datetime
    self.check_flink_status(status_params.flink_pid_file)

  def set_conf_bin(self, env):
    import params
    if params.setup_prebuilt:
      params.conf_dir =  params.flink_install_dir+ '/conf'
      params.bin_dir =  params.flink_install_dir+ '/bin'
    else:
      params.conf_dir =  glob.glob(params.flink_install_dir+ '/flink-dist/target/flink-*/flink-*/conf')[0]
      params.bin_dir =  glob.glob(params.flink_install_dir+ '/flink-dist/target/flink-*/flink-*/bin')[0]
    

  def install_mvn_repo(self):
    #for centos/RHEL 6/7 maven repo needs to be installed
    distribution = platform.linux_distribution()[0].lower()
    if distribution in ['centos', 'redhat'] and not os.path.exists('/etc/yum.repos.d/epel-apache-maven.repo'):
      Execute('curl -o /etc/yum.repos.d/epel-apache-maven.repo https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo')

  def create_hdfs_user(self, user):
    Execute('hadoop fs -mkdir -p /user/'+user, user='hdfs', ignore_failures=True)
    Execute('hadoop fs -chown ' + user + ' /user/'+user, user='hdfs')
    Execute('hadoop fs -chgrp ' + user + ' /user/'+user, user='hdfs')
          
if __name__ == "__main__":
  Master().execute()
