package Parallel::parallel_map;

use 5.008;
our $VERSION = '0.01';

use strict;
use warnings;
use File::Temp qw/tempfile/;

require Exporter;

our @ISA = qw(Exporter);

our @EXPORT = qw(parallel_map);

my $number_of_cpu_cores; 
sub number_of_cpu_cores {
    return $number_of_cpu_cores if $number_of_cpu_cores;
    # this works correct only in unix environment. cygwin as well.
    $number_of_cpu_cores = scalar grep m{^processor\t:\s\d+\s*$},`cat /proc/cpuinfo`;
    die "Can't find out number of cpu cores! Are you in HELL?" unless $number_of_cpu_cores;
}

my $store;
my $retrieve;

# this inits freeze and thaw with Storable subroutines and try to replace them with Sereal counterparts
sub _init_serializer {
    my $param = shift;
    if (exists $param->{freeze} && ref($param->{freeze}) eq 'CODE' &&  exists $param->{thaw} && ref($param->{thaw}) eq 'CODE') {
        $store = $param->{store};
        $retrieve = $param->{retrieve};
    } else {
        # try cereal 
        #eval q{use Sereal qw(encode_sereal decode_sereal);$freeze = \&encode_sereal;$thaw = \&decode_sereal;};
        eval q{use Storable qw(store retrieve);$store = \&store;$retrieve = \&retrieve;} unless $store;
    }
    # don't make any assumptions on serializer capabilities, give all the power to user ;)
    # die "bad serializer!" unless join(",",@{$thaw->($freeze->([1,2,3]))}) eq '1,2,3';
}
_init_serializer({});

sub parallel_map(&@) {
    my ($code,@array) = @_;
    # spawn workers
    my $number_of_workers = number_of_cpu_cores;
    my $items_per_worker = int( (@array + $number_of_workers - 1) / $number_of_workers );
    my $items_per_main = @array - $items_per_worker * ($number_of_workers-1);
    debug('number_of_workers:%d,items_per_worker:%d,items_per_main:%d',$number_of_workers,$items_per_worker,$items_per_main);
    my @store_tmp;
    my %child_index; # {pid=>index}
    my $index_worker = $number_of_workers;
    my @result;
    die "not defined storable!" unless grep(ref($_) eq 'CODE', $store,$retrieve) == 2;
    while ($index_worker--) {
        if ($index_worker == 0) {
            # first part - do it yourself
            @result = map $code->($_),@array[0..$items_per_main-1];
            last;
        }
        my $tmp = File::Temp->new(UNLINK=>0);
        my $key = $tmp->filename;
        #$tmp->unlink_on_destroy( 0 );
        my $pid = fork();
        if ($pid == 0) {
            my ($cur,$next) = map $items_per_main+$items_per_worker * $_, $index_worker-1, $index_worker;
            debug('process items from %d to %d',$cur,$next-1);
            my @result = map $code->($_), @array[$cur..$next-1];
            debug("%d store to %s",$index_worker,$key);
            $store->(\@result,$key);
            #close($fh);
            #print "written file, check existance!\n";<>;
            exit;
        }
        push @store_tmp,$tmp;
        $child_index{$pid} = $index_worker-1;
    }
    # now merge results
    my @results;
    while ((my $pid = wait) != -1) {
        debug('pid:%s',$pid);
        debug('child_index: %s, $child_index{$pid}.defined? %s',\%child_index,defined($child_index{$pid})?'yes':'no');
        my $i = $child_index{$pid};
        debug('i=%d',$i);
        $DB::single = 2;
        my $tmp = $store_tmp[$i];
        my $key = $tmp->filename;
        $results[$i] = $retrieve->($key);
        debug("restore %d from key %s,restored results:%s",$i,$key,$results[$i]);
        $tmp->unlink1($key);
    }
    for my $result (@results) {
        push @result, @$result;
    }
    return @result;
}

use Data::Dumper;
sub debug {
    return;
	my $fmt= shift;
	printf STDERR "$$:$fmt\n",map ref($_)?Dumper($_):$_,@_;
}

1;

__END__

=head1 NAME

Parallel::parallel_map - really parallel calculation in Perl

=head1 SYNOPSIS

  use Parallel::parallel_map;
  my @m2 = parallel_map {$_*2} 1..10000000;

=head1 DESCRIPTION

You know from the school that nothing is more simple than 2 x 2 = 4
You know from the university that it is the simplest operation for computer as well (left shift).
This module tries outperform and speed up even simplest calculations using
all the power of your server CPU cores and familiar map conception.

It
1) finds out how many cpu cores you have,
2) split map work by the number of cores,
3) do it in parallel and
4) after job is done by each thread it merges the results into array.

Sorry, slightly more then 1-2-3


=head2 EXPORT

parallel_map - that what this module is about

=head1 SEE ALSO

Parallel::Loops, MCE, Parallel::DataPipe

=head1 AUTHOR

Oleksandr Kharchenko, <okharch@gmail.com>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by Oleksandr Kharchenko

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.16.3 or,
at your option, any later version of Perl 5 you may have available.


=cut
