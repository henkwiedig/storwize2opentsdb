#!/usr/bin/perl
use XML::LibXML;
use Data::Dumper;
use DateTime::Format::Strptime;
use DateTime;

my $parser = XML::LibXML->new->parse_file($ARGV[0]);
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
my $parser = DateTime::Format::Strptime->new(
  pattern => '%Y-%m-%d %H:%M:%S %O',
  on_error => 'croak',
);
$now = $parser->parse_datetime($timestamp . " " . $timezone);


#iterate over instances
for my $instance ($xml->findnodes('/driveStats:diskStatsColl/driveStats:mdsk')) {
  @attrs = $instance->attributes();
  for my $metric (@attrs[2 .. $#attrs]) {
    print "put storwize.iostat.".$base_metric.".".$metric->nodeName()." ".$now->epoch()." ". $metric->getValue(). 
          " idx=".$attrs['idx']->getValue(). 
          " cluster=".$cluster. 
          " node=".$node.
          " scope=".$scope.  
          "\n";
    
  }
}


