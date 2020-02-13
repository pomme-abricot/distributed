# file that regroup knowledge of all sites and vms - 
import csv
import os.path
import os
import json
import imp

from cassandra.cluster import Cluster
# from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

from sshtunnel import SSHTunnelForwarder
from pymongo.errors import ConnectionFailure
from sshtunnel import BaseSSHTunnelForwarderError


class CloudSiteCassandra():
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
            device_name set<text>, 
            site text, 
            cost_cpu float,
            cost_storage float,
            storage_total float,
            storage_available float,
            transfer_rate text,
            PRIMARY KEY (site) 
            )"""
            self.cloudsite.execute(cmd)

            # cmd = """CREATE TABLE IF NOT EXISTS vm_info ( 
            # device_name text, 
            # site text, 
            # cost_cpu float,
            # cost_storage float,
            # storage_total float,
            # storage_available float,
            # PRIMARY KEY (site) 
            # )"""
            # self.cloudsite.execute(cmd)

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

    def add_data(self, device_name="", site="", cost_cpu=0., cost_storage=0.,
            storage_total=0., storage_available=0., transfer_rate="", dict_item=""):
        # FIRST: FORMAT INPUT:
        if dict_item:
            device_name = dict_item['device_name']
            site = dict_item['site']
        if type(device_name) is not set:
            if type(device_name) is list:
                device_name = set(device_name)
            else:
                device_name = set([device_name])
        transfer_rate = json.dumps(transfer_rate)

        # IF there is an entry for this data: update
        row = self.cloudsite.execute("""SELECT site FROM cloudsite
        WHERE site=%s""", [site])
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
            INSERT INTO cloudsite (device_name, site, cost_cpu, cost_storage, storage_total, storage_available, transfer_rate)
            VALUES (%(device_name)s, %(site)s, %(cost_cpu)s, %(cost_storage)s, %(storage_total)s, %(storage_available)s, %(t_r)s)
            """, consistency_level=ConsistencyLevel.ONE)
            self.cloudsite.execute(query, dict(device_name=device_name, site=site, cost_cpu=cost_cpu, 
                cost_storage=cost_storage, storage_total=storage_total, storage_available=storage_available, t_r=transfer_rate))


    def add_vm(self, device_name="", site=""):
        if type(device_name) is not set:
            device_name = set([device_name])

        row = self.cloudsite.execute("""SELECT site FROM cloudsite
        WHERE site=%s""", [site])
        if row:
            query = SimpleStatement("""
            UPDATE cloudsite 
            SET device_name = device_name + %(device_name)s
            WHERE site=%(site)s
            """, consistency_level=ConsistencyLevel.ONE)
            self.cloudsite.execute(query, dict(device_name=device_name, site=site))
        else:
            pass

    # def set_transfer_rate(self, variable="", value="", site=""):
    #     row = self.cloudsite.execute("""SELECT site FROM cloudsite
    #     WHERE site=%s""", [site])
    #     if row:
    #         query = SimpleStatement("""
    #         UPDATE cloudsite 
    #         SET %(var)s = %(value)s
    #         WHERE site=%(site)s
    #         """, consistency_level=ConsistencyLevel.ONE)
    #         self.cloudsite.execute(query, dict(var=variable, value=value, site=site))
    #     else:
    #         pass

    def get_transfer_rate(self, site=""):
        row = self.cloudsite.execute("""SELECT transfer_rate FROM cloudsite WHERE site=%s""", [site])
        return row


    def update_storage_availability(self, storage_use="", site=""):
        """ Remove the amount of storage to the available storage
        Inputs: [float] storage_use = storage amount to set "not available" (in B)
        """
        remaining = self.get_storage_available(site)
        new_remaining = remaining - storage_use
        row = self.cloudsite.execute("""SELECT site FROM cloudsite
        WHERE site=%s""", [site])
        if row:
            query = SimpleStatement("""
            UPDATE cloudsite 
            SET storage_available =  %(new_remaining)s
            WHERE site=%(site)s
            """, consistency_level=ConsistencyLevel.ONE)
            self.cloudsite.execute(query, dict(new_remaining=new_remaining, site=site))
        else:
            pass


    def get_storage_available(self, site=""):
        row = self.cloudsite.execute("""SELECT storage_available FROM cloudsite
        WHERE site=%s""", [site])
        if row:

            return row[0].storage_available
        else:
            return False

    def get_site_available(self, storage_to_store=""):
        """ Get the list of sites whose storage availability is > storage to add
        Inputs: [float] storage_to_store = the size of data of add to the cache
        """
        row = self.cloudsite.execute("""SELECT site, storage_available, cost_storage FROM cloudsite
        WHERE storage_available>%s 
        ALLOW FILTERING""", [storage_to_store])
        if row:
            return row
        else:
            return False

    def remove_all_data(self):
        query = SimpleStatement("""
        TRUNCATE cloudsite
        """)
        self.cloudsite.execute(query)

    def show_all(self):
        count = self.cloudsite.execute("select count(*) from cloudsite")[0].count
        print "Multisite cloud has:  ", count, " sites."
        if count == 0:
            return
        else:
            query = "SELECT * FROM cloudsite"
            datas = self.cloudsite.execute(query)
            if datas:
                for data in datas:
                    print "site : ", data.site, " has ", str(len(data.device_name)) , " VMs: ", data.device_name, " it has : ", str(data.storage_available), \
                            " available storage on : ", str(data.storage_total), " total storage"

    def find_one(self, site):
        query = """SELECT device_name FROM cloudsite
        WHERE site=%s
        """
        return self.cloudsite.execute(query, [site])

    def empty(self):
        query = """SELECT * FROM cloudsite
        """
        row = self.cloudsite.execute(query)
        if row:
            return False
        else:
            return True

    def get_site(self, device_name):
        # get site from a VM name
        query = """SELECT * FROM cloudsite 
        WHERE device_name CONTAINS %s
        ALLOW FILTERING;
        """
        r = self.cloudsite.execute(query, [device_name])
        if r:
            return str(r[0].site)
        else:
            return False

def start_cloudsite(cloudsite_config=None, cloudsite_type="Cassandra"):
    if cloudsite_type == "Cassandra":
        cloudsite = CloudSiteCassandra()
        cloudsite.init(cloudsite_config)
        return cloudsite