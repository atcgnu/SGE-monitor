#!/usr/bin/perl

#***************************************************************************************************
# FileName: 
# Creator: Chen Y.L. <shenyulan@genomics.cn>
# Create Time: Sun Jul  2 19:18:53 CST 2017

# Description:
# CopyRight: 
# vision: 0.1
# ModifyList:
#   Revision: 
#   Modifier:
#   ModifyTime: 
#   ModifyReason: 
#***************************************************************************************************
use strict;
use warnings;
use File::Basename;


my $usage=<<usage;
    Usage: perl $0 <IN|job.sh>
    Example:
usage

die($usage) unless @ARGV >0;

my ($group, $outdir) = @ARGV;
&group_monitor(2,$group);

my @sge_status = ('total', 'r', 'qw', 'hqw', 's', 'Eqw', 'dr', 'T', 'hr', 't', 'ds');
my (%cpus, %vfree, %mems, %jobstat);


sub group_monitor{
    my ($hibernation, $group) = @_;
    my %job_monitors;
# sudo lfs quota -gh ST_MCHRI_DISEASE  /ldfssz1/ST_MCHRI/DISEASE/
    my $user_list = `grep '$group' /etc/group | awk -F':' '{print \$NF}' | xargs | tr ' ' ','`;
#   my $user_list = "ST_MCHRI_DISEASE,huangxiaoyan3,linqiongfen,zhengyu,";

    my @users = (split /,/, $user_list);
    shift @users;

    reDo:
    {
        my %jobs;
        my $stat;
        sleep $hibernation;
#       "CLINIC", "DISEASE", "REHEAL", "STEMCELL", "BIGDATA", "REPRO");
        my @storages = ("CLINIC", "DISEASE", "REHEAL", "STEMCELL", "REPRO");

        @sge_status = ('total', 'r', 'qw', 'hqw', 's', 'Eqw', 'dr', 'T', 'hr', 't', 'ds');
        (%cpus, %vfree, %mems, %jobstat) = ((), (), ());

        print "* storage *\n";

        foreach my $disk (@storages){
#           my $path = "/hwfssz1/ST_MCHRI/$disk";
#           $path =~ s/\/$//g;
            print "\n";
            my ($total, $used, $avail, $use_p);
            if(-e "/hwfssz1/ST_MCHRI/$disk"){
                ($total, $used, $avail, $use_p) = &get_storage("$disk", "/hwfssz1/ST_MCHRI/$disk", 'wh');
                print "/hwfssz1/ST_MCHRI/$disk: $total, $used, $avail, $use_p\n";
            }

            if(-e "/zfssz3/ST_MCHRI/$disk"){
                ($total, $used, $avail, $use_p) = &get_storage("$disk", "/zfssz3/ST_MCHRI/$disk", 'ld');
                print "/zfssz3/ST_MCHRI/$disk: $total, $used, $avail, $use_p\n";
            }

            if(-e "/zfssz2/ST_MCHRI/$disk"){
                ($total, $used, $avail, $use_p) = &get_storage("$disk", "/zfssz2/ST_MCHRI/$disk", 'ld');
                print "/zfssz2/ST_MCHRI/$disk: $total, $used, $avail, $use_p\n";
            }
            print "\n";
        }

        map{
            $stat .= `qstat -u $_`;
        }(@users);

#       print "$stat\n";
# job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID 
# -----------------------------------------------------------------------------------------------------------------
# 3115440 0.50225 work.sh    huangxiaoyan r     06/29/2017 16:36:56 st.q@cngb-compute-f21-5.cngb.s     1        
# 3118181 0.50225 work.sh    huangxiaoyan r     06/29/2017 17:59:23 st.q@cngb-compute-f23-3.cngb.s     1        
# 3611710 0.50450 work.sh    huangxiaoyan qw    06/29/2017 17:43:41                                    1    

        foreach my $job (split /\n/, $stat){
            $job =~s/^\s+//g;
            next unless $job =~ /^\d+/;
            my ($job_ID, $prior, $name, $user, $state, $st_date, $st_time, $node) = (split /\s+/,$job);
            $jobs{$job_ID}{'state'} = $state;
            $jobs{$job_ID}{'node'} = '?';
            $jobs{$job_ID}{'node'} = $node if $state eq 'r';
        }

print "* job *\n";
        map{
            my $log = "$outdir/qstat.log";
            `qstat -j $_ > $log 2> /dev/null`;
            my ($job_number, $owner, $queue, $project, $script, $cwd, $num_proc, $virtual_free, $cpu, $vmem, $submission_time) = &parseQstat("$log");
            print "$job_number\t$jobs{$job_number}{'state'}\t$jobs{$job_number}{'node'}\t$owner\t$queue\t$project\t$num_proc\t$virtual_free\t$cpu\t$vmem\t$submission_time\t$script\t$cwd\n" if $job_number =~ /^\d+/;
            &get_stat($job_number, $jobs{$job_number}{'state'}, $jobs{$job_number}{'node'}, $owner, $queue, $project, $num_proc, $virtual_free, $cpu, $vmem, $submission_time, $script, $cwd) if $job_number =~ /^\d+/;
        }(keys %jobs);

print "\n* stat *\n";

        print join "\t", @sge_status;
        print "\tuser\tnum_proc\tvirtual_free\tvmem\n";

        foreach my $user (sort {$jobstat{$b}{'total'}<=>$jobstat{$a}{'total'}} (keys %jobstat)){
            my $status_info;
            map{
                my $stat = $jobstat{$user}{$_} || 0;
                $status_info .= "$stat\t";
            }(@sge_status);
            print "$status_info$user\t$cpus{$user}\t$vfree{$user}\t$mems{$user}\n";
        }

    }
}

sub get_storage {
    my ($group, $path, $storage_type) = @_;
    my $suffix = "_".$group if $group =~ /\w/;
    if ($storage_type eq 'wh'){
        my $disk_space = `df -Th $path`;
        chomp $disk_space;
        my $info = (split /\n/, $disk_space)[-1];
        my ($total, $used, $avail, $use_p) = (split /\s+/, $info)[-5, -4, -3,-2];
        return ($total, $used, $avail, $use_p);
    }elsif($storage_type eq 'ld'){
        my $disk_space = `sudo lfs quota -g ST_MCHRI$suffix $path`;
        chomp $disk_space;
        my $info = (split /\n/, $disk_space)[-1];
        my ($total, $used) = (split /\s+/, $info)[-6, -8];

        my $avail = sprintf("%.f", ($total - $used)/(1024*1024*1024));
        my $t_total = sprintf("%.f", $total/(1024*1024*1024));
        my $t_used = sprintf("%.f", $used/(1024*1024*1024));
        my $use_p = sprintf("%.f", $used/$total * 100);

        return ("${t_total}T", "${t_used}T", "${avail}T", "${use_p}%");
    }else{
        print "someting was wrong, please check the type of storage ......\n";
    }
}

sub parseQstat {
    my ($log)= @_;
    my ($job_number, $submission_time, $num_proc, $virtual_free, $owner, $queue, $project, $script, $cwd)=('?','?', 1, '?','?','?','?', '?','?','?');
    my ($cpu, $mem, $io, $vmem, $maxvem) = ('?','?','?','?','?');
    my $now_time = `date`;
    chomp $now_time;
    open LOG, "$log" or die $!;
    while(<LOG>){
        chomp;
        $job_number = $1 if /job_number:\s+(\d+)/;
        $submission_time = $1 if /submission_time:\s+(.*)/;
        $owner = $1 if /owner:\s+(\S+)/;
        $queue = $1 if /hard_queue_list:\s+(\S+)/;
        $project = $1 if /project:\s+(\S+)/;
        $script = $1 if /script_file:\s+(\S+)/;
        $cwd = $1 if /cwd:\s+(\S+)/;
        if(/hard resource_list:\s+(\S+)/){
            my $resource = $1;
            map{$num_proc = $1 if /num_proc=(\S+)/; $virtual_free = $1 if /virtual_free=(\S+)/;}(split /,/, $resource);
        }
#cpu=8:11:52:22, mem=5715315.94808 GBs, io=3786.42584, vmem=11.057G, maxvmem=11.370G
       ($cpu, $mem, $io, $vmem, $maxvem)= ($1, $2, $3, $4, $5) if /usage    1:\s+cpu=(\S+), mem=(.*), io=(\S+), vmem=(\S+), maxvmem=(\S+)/;
        last if /scheduling info/;
    }
    close LOG;
    return ($job_number, $owner, $queue, $project, $script, $cwd, $num_proc, $virtual_free, $cpu, $vmem, $submission_time);
}


sub get_stat {
    my ($job_number, $job_status, $job_node, $owner, $queue, $project, $num_proc, $virtual_free, $cpu_time, $vmem, $submission_time, $script, $cwd) = @_;
    $vmem = 0 if $vmem eq 'N/A';
    $vmem = 0 if $vmem =~ /\?/;
    $vmem =~ s/M// if $vmem =~ /M$/;
    $vmem = $vmem/1024 if $vmem =~ /M$/;
    $vmem =~ s/g//i;

    $virtual_free = 0 if $virtual_free eq 'N/A';
    $virtual_free = 0 if $virtual_free =~ /\?/;
    $virtual_free =~ s/M// if $virtual_free =~ /M$/;
    $virtual_free = $virtual_free/1024 if $virtual_free =~ /M$/;
    $virtual_free =~ s/g//i;

    $cpus{$owner} += $num_proc;
    $cpus{'total'} += $num_proc;

    $mems{$owner} += $vmem;
    $mems{'total'} += $vmem;

    $vfree{$owner} += $virtual_free;
    $vfree{'total'} += $virtual_free;

    $jobstat{$owner}{$job_status} ++;
    $jobstat{$owner}{'total'} ++;
    $jobstat{'total'}{'total'} ++;
    $jobstat{'total'}{$job_status} ++;
}
