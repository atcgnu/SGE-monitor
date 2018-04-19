#!/usr/bin/perl

use strict;
use warnings;
use Proc::Daemon;

Proc::Daemon::Init;

my $continue = 1;
$SIG{TERM} = sub { $continue = 0 };

my $now = `date +%s`;
my $hibernation = 0;
#my $hibernation = 1499000400 - $now;
#my $hibernation = 100;
`sleep $hibernation`;
#my $three_hour = 300;
my $three_hour = 3600 * 3;
my $i = 0;
while ($continue) {
    $i ++;
#   (data + i * 3) - now
    my ($sec,$min,$hour,$mday,$mon,$year_off,$wday,$yday,$isdat) = localtime(time);
    my $year = $year_off + 1990;
    my $log = "qstat.$year-$mon-$mday-$hour-$i.log";
    `perl allmonitor.pl  ST_MCHRI_DISEASE $monitor_log_dir  2&>> $log`;
    `sleep $three_hour`;
}
