# file that regroup knowledge of all sites and vms - 
import csv
import os.path
import os

from cassandra.cluster import Cluster
# from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

from sshtunnel import SSHTunnelForwarder
from pymongo.errors import ConnectionFailure
from sshtunnel import BaseSSHTunnelForwarderError


class CLoudSiteCassandra():
    def __init__(self):
        self.cloudsite = None
        self.server = None
        self.remote = None
        
    def start_client(self, *args, **kwargs):
        # Tunnel a port with ssh toward the cassandra server
        if self.remote:
            if not self.server:
                print "SSH Server not started - cannot connect to Cassandra"
                return
            cassandra_port = self.server.local_bind_port
        else:
            cassandra_port = kwargs.get('cassandra_port')

        try:
            cluster = Cluster(['127.0.0.1'], port=cassandra_port)
            session = cluster.connect()
            self.cloudsite = session
            KEYSPACE = "wf_metadata"
            # create keyspace and table if it doesnt exist already
            cmd = """CREATE  KEYSPACE IF NOT EXISTS wf_metadata 
            WITH REPLICATION = { 
            'class' : 'SimpleStrategy',
            'replication_factor' : 1 
            }"""
            self.cloudsite.execute(cmd)
            self.cloudsite.set_keyspace(KEYSPACE)

            # TODO: MAKE THE PATH A LIST OF THE PATHS WHERE THE DATA EXISTS
            cmd = """CREATE TABLE IF NOT EXISTS cloudsite ( 
            device_name text, 
            site text, 
            PRIMARY KEY (device_name) 
            )"""
            self.cloudsite.execute(cmd)
        except ConnectionFailure:
            print "failed to connect to Cassandra"

    def close(self):
        pass

    def delete(self):
        cmd = """DROP TABLE IF EXISTS cloudsite;
        """
        self.cloudsite.execute(cmd)

    def start_sshtunnel(self, *args, **kwargs):
        try:
            self.server = SSHTunnelForwarder(
                ssh_address_or_host=kwargs.get('ssh_ip_addr'),
                ssh_pkey=kwargs.get('ssh_pkey'),
                ssh_username=kwargs.get('ssh_username'),
                remote_bind_address=kwargs.get('remote_bind_address')
                # ,
                # *args,
                # **kwargs
            )

            self.server.start()
        except BaseSSHTunnelForwarderError:
            print "Fail to connect to ssh device"

    def close_sshtunel(self):
        return self.server.stop()

    def init(self, *args, **kwargs):
        path_config =  kwargs.get('cloudsite_config', None)
        if path_config == None:
            import openalea.distributed.index.cloudsite_config as cloudsite_config
        else:
            cloudsite_config = imp.load_source('cloudsite_config', path_config)
            import cloudsite_config
        print("Load cloudsite config from : ", cloudsite_config.__file__)
        if cloudsite_config.REMOTE_SITE:
            self.remote=True
            self.start_sshtunnel(ssh_ip_addr=cloudsite_config.CASSANDRA_SSH_IP,
                                ssh_pkey=cloudsite_config.SSH_PKEY,
                                ssh_username=cloudsite_config.SSH_USERNAME,
                                remote_bind_address=("localhost", cloudsite_config.CASSANDRA_PORT) , 
                                *args, **kwargs)
        else: 
            self.remote=False
        self.start_client(cassndra_ip_addr=cloudsite_config.CASSANDRA_ADDR,
                          cassandra_port=cloudsite_config.CASSANDRA_PORT,
                          *args, **kwargs)

    def add_data(self, device_name="", site="", dict_item=""):
        # FIRST: FORMAT INPUT:
        if dict_item:
            device_name = dict_item['device_name']
            site = dict_item['site']

        # IF there is an entry for this data: update
        row = self.cloudsite.execute("""SELECT site FROM cloudsite
        WHERE device_name=%s""", [device_name])
        if row:
            pass
            # query = SimpleStatement("""
            # UPDATE site 
            # SET path = path + %(p)s,
            # SET site = site + %(site)s,
            # execution_data = %(ed)s,
            # cache_data = %(cd)s
            # WHERE data_id=%(d_id)s
            # """, consistency_level=ConsistencyLevel.ONE)
            # self.site.execute(query, dict(d_id=data_id, p=path, site=site, ed=exec_data, cd=cache_data))

        else:
            query = SimpleStatement("""
            INSERT INTO cloudsite (device_name, site)
            VALUES (%(device_name)s, %(site)s)
            """, consistency_level=ConsistencyLevel.ONE)
            self.cloudsite.execute(query, dict(device_name=device_name, site=site))

    def remove_one_data(self, device_name="", site=""):
        # TODO: NOT DELETE => REMOVE ONE OF THE PATHS IF SEVERAL EXISTS
        query = SimpleStatement("""
        DELETE FROM cloudsite 
        WHERE device_name=%s
        """)
        self.cloudsite.execute(query, [device_name])

    def remove_all_data(self):
        query = SimpleStatement("""
        TRUNCATE cloudsite
        """)
        self.cloudsite.execute(query)

    def show_all(self):
        count = self.cloudsite.execute("select count(*) from cloudsite")[0].count
        print "Multisite cloud has:  ", count, " VMs."
        if count == 0:
            return
        else:
            query = "SELECT * FROM cloudsite"
            datas = self.cloudsite.execute(query)
            if datas:
                for data in datas:
                    print "device : ", data.device_name, " on site: ", data.site, 

    def find_one(self, device_name):
        query = """SELECT site FROM cloudsite
        WHERE device_name=%s
        """
        return self.cloudsite.execute(query, [device_name])

    def empty(self):
        query = """SELECT * FROM cloudsite
        """
        row = self.cloudsite.execute(query)
        if row:
            return False
        else:
            return True

    def get_site(self, device_name):
        query = """SELECT site FROM cloudsite
        WHERE device_name=%s
        """
        r = self.cloudsite.execute(query, [device_name])
        for i in r:
            if i:
                return i.site[0]
            else:
                return False

def start_cloudsite(cloudsite_config=None, cloudsite_type="Cassandra"):
    if cloudsite_type == "Cassandra":
        cloudsite = CloudSiteCassandra()
        cloudsite.init(cloudsite_config)
        return cloudsite