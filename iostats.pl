#!/usr/bin/perl
#
#iostats.pl
#
# see: https://www.ibm.com/developerworks/community/wikis/home?lang=en#!/wiki/W1d985101fbfa_4ae7_a090_dc535355ae7e/page/Fetch+Performance+Stats
#     https://www.ibm.com/support/knowledgecenter/STHGUJ_7.6.1/com.ibm.storwize.tb5.761.doc/svc_clusterstartstatswin_20qm0u.html
#

use XML::LibXML;
use Date::Parse;
use File::Path qw(make_path remove_tree);
use Data::Dumper;
use IO::Socket::INET;

$iostat_dir = '/dev/shm/storwize2opentsdb';
$keep_old_files = 0;
$tsd_host = $ENV{TSD_HOST};
$tsd_port = $ENV{TSD_PORT};
$tsd_file_name = '/dev/shm/storwize2opentsdb.tsdb';

sub read_config {
#we can read stor2rrd config files
  open(FILE,$ARGV[0]);
  while (<FILE>) {
    chomp;
    if ( $_ !~ /#.*/ && $_ !~ /^$/ )  {
	  my ($cluster , $type , $ip, $key ,$VOLUME_AGG_DATA_LIM, $VOLUME_AGG_IO_LIM, $SAMPLE_RATE_MINS) = split (':',$_);
	  $config{$cluster}{'type'} = $type;
	  $config{$cluster}{'ip'} = $ip,
	  $config{$cluster}{'key'} = $key,
	  $config{$cluster}{'VOLUME_AGG_DATA_LIM'} = $VOLUME_AGG_DATA_LIM,
	  $config{$cluster}{'VOLUME_AGG_IO_LIM'} = $VOLUME_AGG_IO_LIM,
	  $config{$cluster}{'SAMPLE_RATE_MINS'} = $SAMPLE_RATE_MINS
	  };
  };
  close(FILE);
}

sub check_and_enable_statistics {
#svcinfo lssystem
#statistics_status on
#statistics_frequency 15
#svctask startstats -interval interval
}

sub copy_iostats_from_non_config_nodes_to_config_node {
#ssh admin@<cluster> "svcinfo lsnode -nohdr -filtervalue config_node=no|while read -a node; \
  #do svctask cpdumps -prefix /dumps/iostats ${node[0]}; done"
#scp admin@<cluster>:/dumps/iostats/* <destination>
}

sub fetch_iostats {
  my $current = shift;
  my $params = shift;
  make_path( $params->{$current}->{'iostatst_dir'} ); 
  $cmd = "scp -i ".$params->{$current}->{'key'}." ".$params->{$current}->{'ip'}.":/dumps/iostats/* ".$params->{$current}->{'iostatst_dir'};
  $result = `$cmd 2>&1`;
}

sub search_files {
  my $current = shift;
  my $params = shift;
  opendir(my $dh,  $params->{$current}->{'iostatst_dir'}) || die "Can't open ".$params->{$current}->{'iostatst_dir'}.": $!";
  @files = sort readdir $dh;
  while ( my $file = shift @files ) {
	  if ($file =~ /^Nd_stats_(.*)_(.*)_(.*)/) {
		$params->{$current}->{'Nd_file'}->{$1} = $file;
	  } elsif ($file =~ /^Nm_stats_(.*)_(.*)_(.*)/) {
		$params->{$current}->{'Nm_file'}->{$1} = $file;
	  } elsif ($file =~ /^Nn_stats_(.*)_(.*)_(.*)/) {
		$params->{$current}->{'Nn_file'}->{$1} = $file;
	  } elsif ($file =~ /^Nv_stats_(.*)_(.*)_(.*)/) {
		$params->{$current}->{'Nv_file'}->{$1} = $file;
	  }
  }
  closedir $dh;
}

sub process_Nd_files {
    my $parser = XML::LibXML->new->parse_file($_[0]);
    my $xml = XML::LibXML::XPathContext->new($parser);
    $xml->registerNs('driveStats','http://ibm.com/storage/management/performance/api/2010/03/driveStats');
    
    
    #read global variables
    $timestamp = $xml->findnodes('/driveStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('timestamp')->getValue();
    $timezone = $xml->findnodes('/driveStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('timezone')->getValue();
    $cluster = $xml->findnodes('/driveStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('cluster')->getValue();
    $node = $xml->findnodes('/driveStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('id')->getValue();
    $scope = $xml->findnodes('/driveStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('scope')->getValue();
    $base_metric = $xml->findnodes('/driveStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('contains')->getValue();
    
    #parse the vaules timestamp
    $now = str2time($timestamp , $timezone);
    
    
    #iterate over instances
    for my $instance ($xml->findnodes('/driveStats:diskStatsColl/driveStats:mdsk')) {
      @attrs = $instance->attributes();
      for my $metric (@attrs[2 .. $#attrs]) {
        print $tsd_file "put storwize.iostat.".$base_metric.".".$metric->nodeName()." ".$now." ". $metric->getValue(). 
              " idx=".$instance->attributes()->getNamedItem('idx')->getValue().
              " cluster=".$cluster. 
              " node=".$node.
              " scope=".$scope.  
              "\n";
        
      }
    }
}

sub process_Nm_files {
    my $parser = XML::LibXML->new->parse_file($_[0]);
    my $xml = XML::LibXML::XPathContext->new($parser);
    $xml->registerNs('diskStats','http://ibm.com/storage/management/performance/api/2003/04/diskStats');
    
    
    #read global variables
    $timestamp = $xml->findnodes('/diskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('timestamp')->getValue();
    $timezone = $xml->findnodes('/diskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('timezone')->getValue();
    $cluster = $xml->findnodes('/diskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('cluster')->getValue();
    $node = $xml->findnodes('/diskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('id')->getValue();
    $scope = $xml->findnodes('/diskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('scope')->getValue();
    $base_metric = $xml->findnodes('/diskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('contains')->getValue();
    
    #parse the vaules timestamp
    $now = str2time($timestamp , $timezone);
    
    
    #iterate over instances
    for my $instance ($xml->findnodes('/diskStats:diskStatsColl/diskStats:mdsk')) {
      @attrs = $instance->attributes();
      for my $metric (@attrs[2 .. $#attrs]) {
        print $tsd_file "put storwize.iostat.".$base_metric.".".$metric->nodeName()." ".$now." ". $metric->getValue(). 
              " idx=".$instance->attributes()->getNamedItem('idx')->getValue(). 
              " cluster=".$cluster. 
              " node=".$node.
              " scope=".$scope.  
              " name=".$instance->attributes()->getNamedItem('id')->getValue().  
              "\n";
      }
      #each instances has a cache child
      for my $ca ($instance->findnodes('./*')) {
        @attrs = $ca->attributes();
        for my $metric (@attrs) {
          print $tsd_file "put storwize.iostat.".$base_metric.".cache.".$metric->nodeName()." ".$now." ". $metric->getValue(). 
                " idx=".$instance->attributes()->getNamedItem('idx')->getValue(). 
                " cluster=".$cluster. 
                " node=".$node.
                " scope=".$scope.  
                " name=".$instance->attributes()->getNamedItem('id')->getValue().  
                "\n";
	  }
      }
   }
}

sub process_Nn_files {
    my $parser = XML::LibXML->new->parse_file($_[0]);
    my $xml = XML::LibXML::XPathContext->new($parser);
    $xml->registerNs('nodeStats','http://ibm.com/storage/management/performance/api/2006/01/nodeStats');
    
    
    #read global variables
    $timestamp = $xml->findnodes('/nodeStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('timestamp')->getValue();
    $timezone = $xml->findnodes('/nodeStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('timezone')->getValue();
    $cluster = $xml->findnodes('/nodeStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('cluster')->getValue();
    $node = $xml->findnodes('/nodeStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('id')->getValue();
    $scope = $xml->findnodes('/nodeStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('scope')->getValue();
    $base_metric = $xml->findnodes('/nodeStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('contains')->getValue();
    
    #parse the vaules timestamp
    $now = str2time($timestamp , $timezone);
    
    
    #iterate over instances cpu
    for my $instance ($xml->findnodes('/nodeStats:diskStatsColl/nodeStats:cpu')) {
      @attrs = $instance->attributes();
      for my $metric (@attrs) {
        print $tsd_file "put storwize.iostat.".$base_metric.".cpu.".$metric->nodeName()." ".$now." ". $metric->getValue(). 
              " cluster=".$cluster. 
              " node=".$node.
              " scope=".$scope.  
              "\n";
        
      }
    }
    #iterate over instances cpu_core
    for my $instance ($xml->findnodes('/nodeStats:diskStatsColl/nodeStats:cpu_core')) {
      @attrs = $instance->attributes();
      for my $metric (@attrs[1 .. $#attrs]) {
        print $tsd_file "put storwize.iostat.".$base_metric.".cpu_core.".$metric->nodeName()." ".$now." ". $metric->getValue(). 
              " cpu_id=".$instance->attributes()->getNamedItem('id')->getValue().
              " cluster=".$cluster. 
              " node=".$node.
              " scope=".$scope.  
              "\n";
        
      }
   }
   #iterate over instances node
    for my $instance ($xml->findnodes('/nodeStats:diskStatsColl/nodeStats:node')) {
      @attrs = $instance->attributes();
      for my $metric (@attrs[4 .. $#attrs]) {
        print $tsd_file "put storwize.iostat.".$base_metric.".node.".$metric->nodeName()." ".$now." ". $metric->getValue(). 
              " node_id=".$instance->attributes()->getNamedItem('id')->getValue().
              " cluster=".$cluster. 
              " node=".$node.
              " scope=".$scope.  
              "\n";
        
      }
   }
   #iterate over instances port
    for my $instance ($xml->findnodes('/nodeStats:diskStatsColl/nodeStats:port')) {
      @attrs = $instance->attributes();
      for my $metric (@attrs[8 .. $#attrs]) {
        print $tsd_file "put storwize.iostat.".$base_metric.".port.".$metric->nodeName()." ".$now." ". $metric->getValue(). 
              " port_id=".$instance->attributes()->getNamedItem('id')->getValue().
              " cluster=".$cluster. 
              " node=".$node.
              " scope=".$scope.  
              "\n";
        
      }
   }
   #iterate over instances uca.ca
    for my $instance ($xml->findnodes('/nodeStats:diskStatsColl/nodeStats:uca/nodeStats:ca')) {
      @attrs = $instance->attributes();
      for my $metric (@attrs[0 .. $#attrs]) {
        print $tsd_file "put storwize.iostat.".$base_metric.".uca.ca.".$metric->nodeName()." ".$now." ". $metric->getValue(). 
              " cluster=".$cluster. 
              " node=".$node.
              " scope=".$scope.  
              "\n";
        
      }
   }
   #iterate over instances uca.partition
    for my $instance ($xml->findnodes('/nodeStats:diskStatsColl/nodeStats:uca/nodeStats:partition')) {
      #each partition has a cache child
      @cas = $instance->findnodes('./*');
      for my $ca (@cas[0 .. $#cas]) {
        @attrs = $ca->attributes();
        for my $metric (@attrs[0 .. $#attrs]) {
          print $tsd_file "put storwize.iostat.".$base_metric.".uca.partition.".$metric->nodeName()." ".$now." ". $metric->getValue(). 
                " partition=".$instance->attributes()->getNamedItem('mdg')->getValue().
                " cluster=".$cluster. 
                " node=".$node.
                " scope=".$scope.  
                "\n";
        
        }
     }
  }
   #iterate over instances lca.partition
    for my $instance ($xml->findnodes('/nodeStats:diskStatsColl/nodeStats:lca/nodeStats:partition')) {
      #each partition has a cache child
      @cas = $instance->findnodes('./*');
      for my $ca (@cas[0 .. $#cas]) {
        @attrs = $ca->attributes();
        for my $metric (@attrs[0 .. $#attrs]) {
          print $tsd_file "put storwize.iostat.".$base_metric.".lca.partition.".$metric->nodeName()." ".$now." ". $metric->getValue(). 
                " partition=".$instance->attributes()->getNamedItem('mdg')->getValue().
                " cluster=".$cluster. 
                " node=".$node.
                " scope=".$scope.  
                "\n";
        
        }
     }
  }
}

sub process_Nv_files {
    my $parser = XML::LibXML->new->parse_file($_[0]);
    my $xml = XML::LibXML::XPathContext->new($parser);
    $xml->registerNs('virtualDiskStats','http://ibm.com/storage/management/performance/api/2005/08/vDiskStats');
    
    
    #read global variables
    $timestamp = $xml->findnodes('/virtualDiskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('timestamp')->getValue();
    $timezone = $xml->findnodes('/virtualDiskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('timezone')->getValue();
    $cluster = $xml->findnodes('/virtualDiskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('cluster')->getValue();
    $node = $xml->findnodes('/virtualDiskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('id')->getValue();
    $scope = $xml->findnodes('/virtualDiskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('scope')->getValue();
    $base_metric = $xml->findnodes('/virtualDiskStats:diskStatsColl')->get_node(1)->attributes->getNamedItem('contains')->getValue();
    
    #parse the vaules timestamp
    $now = str2time($timestamp , $timezone);
    
    
    #iterate over instances vdisk
    for my $instance ($xml->findnodes('/virtualDiskStats:diskStatsColl/virtualDiskStats:vdsk')) {
      @attrs = $instance->attributes();
      for my $metric (@attrs) {
        print $tsd_file "put storwize.iostat.".$base_metric.".".$metric->nodeName()." ".$now." ". $metric->getValue(). 
              " vdisk=".$instance->attributes()->getNamedItem('id')->getValue().
              " vdisk_id=".$instance->attributes()->getNamedItem('idx')->getValue().
              " cluster=".$cluster. 
              " node=".$node.
              " scope=".$scope.  
              "\n" unless $metric->nodeName() =~ /id/;
        
      }
	  #each instances has a cache child
	  for my $ca ($instance->childNodes()->get_node(2)) {
        @attrs = $ca->attributes();
        for my $metric (@attrs) {
          print $tsd_file "put storwize.iostat.".$base_metric.".cache.".$metric->nodeName()." ".$now." ". $metric->getValue().
                " vdisk=".$instance->attributes()->getNamedItem('id')->getValue().
                " vdisk_id=".$instance->attributes()->getNamedItem('idx')->getValue().
                " cluster=".$cluster. 
                " node=".$node.
                " scope=".$scope.  
                "\n";
	  }
      }
	  #iterate over vdisk copys
	  for my $copy ($instance->getElementsByTagNameNS('*','cpy')) {
	    for my $ca ($instance->childNodes()->get_node(2)) {
          @attrs = $ca->attributes();
          for my $metric (@attrs) {
            print $tsd_file "put storwize.iostat.".$base_metric.".copy.cache.".$metric->nodeName()." ".$now." ". $metric->getValue(). 
                  " vdisk=".$instance->attributes()->getNamedItem('id')->getValue().
                  " vdisk_id=".$instance->attributes()->getNamedItem('idx')->getValue().
                  " cluster=".$cluster. 
                  " node=".$node.
                  " scope=".$scope.  
				  " copy_id=".$copy->attributes()->getNamedItem('idx')->getValue().
                  "\n";
		  }
	    }
    }
	}
}

read_config();

open($tsd_file, '>' ,$tsd_file_name);   

foreach $svc (keys %config)
{
  $config{$svc}->{'iostatst_dir'} = $iostat_dir."/".$svc."/";
  fetch_iostats($svc,\%config);
  search_files($svc,\%config);
  foreach $Nd_file (keys $config{$svc}->{'Nd_file'}) {
    process_Nd_files($config{$svc}->{'iostatst_dir'} . $config{$svc}->{'Nd_file'}->{$Nd_file});
  }
  foreach $Nm_file (keys $config{$svc}->{'Nm_file'}) {
    process_Nm_files($config{$svc}->{'iostatst_dir'} . $config{$svc}->{'Nm_file'}->{$Nm_file});
  }
  foreach $Nn_file (keys $config{$svc}->{'Nn_file'}) {
    process_Nn_files($config{$svc}->{'iostatst_dir'} . $config{$svc}->{'Nn_file'}->{$Nn_file});
  }
  foreach $Nv_file (keys $config{$svc}->{'Nv_file'}) {
    process_Nv_files($config{$svc}->{'iostatst_dir'} . $config{$svc}->{'Nv_file'}->{$Nv_file});
  }
}

print $tsd_file "exit\n";

close ($tsd_file);


#open socket to tsd to push data
$tsd = IO::Socket::INET->new(
   PeerAddr   =>   $tsd_host,
   PeerPort   =>   $tsd_port,
   Proto      =>   'tcp'
   );
open($tsd_file, '<' ,$tsd_file_name);
while (<$tsd_file>) {
  print $tsd $_;
}
close ($tsd_file);
$tsd->shutdown(1);
close $tsd;

if (! $keep_old_files) {
   remove_tree($iostat_dir) or warn "Could not unlink $file: $!";
   unlink $tsd_file_name or warn "Could not unlink $file: $!";
}

exit 0;
