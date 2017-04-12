#!/usr/bin/perl
#
#iostat.pl
#
# see: https://www.ibm.com/developerworks/community/wikis/home?lang=en#!/wiki/W1d985101fbfa_4ae7_a090_dc535355ae7e/page/Fetch+Performance+Stats
#

use XML::LibXML;
use Date::Parse;
use File::Path;
use Data::Dumper;

$iostat_dir = '/tmp/storwize2opentsdb';
$keep_old_files = 1;

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
  mkpath( $params->{$current}->{'iostatst_dir'} ); 
  $cmd = "scp -i ".$params->{$current}->{'key'}." ".$params->{$current}->{'ip'}.":/dumps/iostats/* ".$params->{$current}->{'iostatst_dir'};
  $result = `$cmd 2>&1`;
  print $result;
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
    $now = str2time($timestamp . " " . $timezone);
    
    
    #iterate over instances
    for my $instance ($xml->findnodes('/driveStats:diskStatsColl/driveStats:mdsk')) {
      @attrs = $instance->attributes();
      for my $metric (@attrs[2 .. $#attrs]) {
        print "put storwize.iostat.".$base_metric.".".$metric->nodeName()." ".$now." ". $metric->getValue(). 
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
    $now = str2time($timestamp . " " . $timezone);
    
    
    #iterate over instances
    for my $instance ($xml->findnodes('/diskStats:diskStatsColl/diskStats:mdsk')) {
      @attrs = $instance->attributes();
      for my $metric (@attrs[2 .. $#attrs]) {
        print "put storwize.iostat.".$base_metric.".".$metric->nodeName()." ".$now." ". $metric->getValue(). 
              " idx=".$instance->attributes()->getNamedItem('idx')->getValue(). 
              " cluster=".$cluster. 
              " node=".$node.
              " scope=".$scope.  
			  " name=".$instance->attributes()->getNamedItem('id')->getValue().  
              "\n";
		@cas = $instance->childNodes()->get_node(2)->attributes();
		for my $ca (@cas) {
		  print "put storwize.iostat.".$base_metric.".cache.".$ca->nodeName()." ".$now." ". $ca->getValue(). 
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

read_config();
foreach $svc (keys %config)
{
  $config{$svc}->{'iostatst_dir'} = $iostat_dir."/".$svc."/";
  #fetch_iostats($svc,\%config);
  search_files($svc,\%config);
  #foreach $Nd_file (keys $config{$svc}->{'Nd_file'}) {
  #  process_Nd_files($config{$svc}->{'iostatst_dir'} . $config{$svc}->{'Nd_file'}->{$Nd_file});
  #}
  foreach $Nm_file (keys $config{$svc}->{'Nm_file'}) {
    process_Nm_files($config{$svc}->{'iostatst_dir'} . $config{$svc}->{'Nm_file'}->{$Nm_file});
  }
  if (! $keep_old_files) {
    rmtree($config{$svc}->{'iostatst_dir'});
  }
}









