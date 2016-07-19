#! /usr/bin/env ruby
#
#   check-cassandra-nodestats
#
# DESCRIPTION:
#   This plugin uses Apache Cassandra's `nodetool` to capture nodetool status of services
#
# OUTPUT:
#   json statistics
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#   Cassandra's nodetool
#
# USAGE:
#     $ ./check-cassandra-nodestats.rb --ndstatus
#     CheckCassandraNodeStats OK: {"node.172.16.17.1.status":"UN"}

require 'sensu-plugin/check/cli'
require 'socket'
require 'json'

UNITS_FACTOR = {
  'bytes' => 1,
  'KB' => 1024,
  'MB' => 1024**2,
  'GB' => 1024**3,
  'TB' => 1024**4
}

#
# Cassandra Metrics
#
class CheckCassandraNodeStats < Sensu::Plugin::Check::CLI
  option :hostname,
         short: '-h HOSTNAME',
         long: '--host HOSTNAME',
         description: 'cassandra hostname',
         default: 'localhost'

  option :port,
         short: '-P PORT',
         long: '--port PORT',
         description: 'cassandra JMX port',
         default: '7199'

  option :crit,
         short: '-c CRITICAL',
         long: '--crit CRITICAL',
         description: 'Critical level for Event',
         default: 15

  option :gossip,
         short: '-g',
         long: '--gossip',
         description: 'Gossip protocol status',
         boolean: true,
         default: false
         
  option :thrift,
         short: '-t',
         long: '--thrift',
         description: 'Thrift protocol status',
         boolean: true,
         default: false
         
  option :nativetransport,
         short: '-n',
         long: '--native',
         description: 'NativeTransport protocol status',
         boolean: true,
         default: false  
  
  option :ndstatus,
         short: '-d',
         long: '--ndstatus',
         description: 'Nodetool nodes status',
         boolean: true,
         default: false


  # convert_to_bytes(512, 'KB') => 524288
  # convert_to_bytes(1, 'MB') => 1048576
  def convert_to_bytes(size, unit)
    size.to_f * UNITS_FACTOR[unit]
  end

  # execute cassandra's nodetool and return output as string
  def nodetool_cmd(cmd)
    `nodetool -h #{config[:hostname]} -p #{config[:port]} #{cmd}`
  end

  def parse_gossip# rubocop:disable all
    info = nodetool_cmd('info')
    info.each_line do |line|
      if m = line.match(/^Gossip active[^:]+:\s(.+)/)# rubocop:disable all
        status = m[1]
        gossip_attr ={"#{config[:hostname]}.gossip_status" => status}
        critical gossip_attr.to_json if (status != 'true')
        ok gossip_attr.to_json
      end
    end
  end   
  
  
  def parse_thrift# rubocop:disable all
    info = nodetool_cmd('info')
    info.each_line do |line|
      if m = line.match(/^Thrift active[^:]+:\s(.+)/)# rubocop:disable all
        status = m[1]
        thrift_attr ={"#{config[:hostname]}.thrift_status" => status}
        critical thrift_attr.to_json if (status != 'true')
        ok thrift_attr.to_json
      end
    end
  end    
  
  def parse_nativetransport# rubocop:disable all
    info = nodetool_cmd('info')
    info.each_line do |line|
      if m = line.match(/^Native Transport active:\s(.+)/)# rubocop:disable all
        status = m[1]
        nativetransport_attr ={"#{config[:hostname]}.nativetransport_status" => status}
        critical nativetransport_attr.to_json if (status != 'true')
        ok nativetransport_attr.to_json
      end  
    end
  end 
      
  # $ nodetool status
  # Datacenter: LON5
  # ================
  # Status=Up/Down
  # |/ State=Normal/Leaving/Joining/Moving
  # --  Address      Load       Tokens       Owns    Host ID                               Rack
  # UN  172.16.1.1  1.88 GB    256          ?       5uu5274d-0c1c-46f1-b73c-c28ffdcad10e  A12-5
  # DN  172.16.1.2  2.55 GB    256          ?       4uu6478c-0e29-468c-ad38-f417ccbcf403  A12-5
  # UL  172.16.1.3  3.24 GB    256          ?       fuu0063d-a033-4a78-95e8-40a479d99a6b  A12-5
  # UJ  172.16.1.4  4.92 GB    256          ?       1uuace8e-af9c-4eff-9977-1a34c09c5535  A12-5
  # UN  172.16.1.5  5.22 GB    256          ?       7uu9ee6c-f093-4fa0-874b-3f5bcaa5b952  A12-5

  def parse_ndstatus# rubocop:disable all
    nodestatus = nodetool_cmd('status')
    nativetransport_attr = []
    nodestatus.each_line do |line|
      next if line.match(/^Datacenter:/)
      next if line.match(/^================/)
      next if line.match(/^Status=Up/)
      next if line.match(/State=Normal/)
      next if line.match(/^--/)
      
      if m = line.match(/^UN\s\s(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/)# rubocop:disable all
        address = m[1]
        ndstatus_attr = {"node.#{address}.status" => 'UN'}
        ok ndstatus_attr.to_json
      else
        m = line.match(/(\w+)\s\s(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/)
        (ndstatus,address) = m.captures
        ndstatus_attr = {"node.#{address}.status" => ndstatus}
        critical ndstatus_attr.to_json
      end

    end
  end

  def run
    @timestamp = Time.now.to_i
    
    parse_gossip if config[:gossip]
    parse_thrift if config[:thrift]
    parse_nativetransport if config[:nativetransport]
    parse_ndstatus if config[:ndstatus]

    ok
  end
end
