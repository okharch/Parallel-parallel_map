package Parallel::parallel_map;

use 5.008;
our $VERSION = '0.01';

use strict;
use warnings;
use File::Temp qw/tempfile/;

require Exporter;

our @ISA = qw(Exporter);

our @EXPORT = qw(parallel_map);

# this should work with Windows NT or if user explicitly set that
my $number_of_cpu_cores = $ENV{NUMBER_OF_PROCESSORS}; 
sub number_of_cpu_cores {
    $number_of_cpu_cores = $_[0] if @_; # setter
    return $number_of_cpu_cores if $number_of_cpu_cores;
    # this works correct only in unix environment. cygwin as well.
    $number_of_cpu_cores = scalar grep m{^processor\t:\s\d+\s*$},`cat /proc/cpuinfo`;
    # otherwise it sets number_of_cpu_cores to 2
    return $number_of_cpu_cores || 2;
}

my $store;
my $retrieve;

# this inits freeze and thaw with Storable subroutines and try to replace them with Sereal counterparts
sub _init_serializer {
    my ($param) = @_;
    if (grep(exists $param->{$_} && ref($param->{$_}) eq 'CODE',qw(store retrieve)) == 2) {
        $store = $param->{store};
        $retrieve = $param->{retrieve};
    } else {
        # try cereal 
        my $sereal = eval q{use Sereal qw(encode_sereal decode_sereal);use File::Slurp qw(read_file write_file);1};
        if ($sereal) {
            #print "using cereal codecs2\n";
            _assign_sereal_store_retrieve();
        } else {
            eval q{use Storable qw(nstore retrieve);$store = \&nstore;$retrieve = \&retrieve;} unless $store;
        }
    }
    # don't make any assumptions on serializer capabilities, give all the power to user ;)
    # die "bad serializer!" unless join(",",@{$thaw->($freeze->([1,2,3]))}) eq '1,2,3';
}

sub _assign_sereal_store_retrieve {
    $store = sub {
        my ($data,$key) = @_;
        $data = encode_sereal($data);
        write_file( $key, {binmode => ':raw'}, $data ) ;
    };
    $retrieve = sub {
        my ($key) = @_;
        return decode_sereal( scalar read_file( $key, { binmode => ':raw' } ) );
    };
}
_init_serializer({});

sub parallel_map(&@) {
    my ($code,@array) = @_;
    my $wantresult = wantarray;
    # spawn workers
    my $number_of_workers = number_of_cpu_cores;
    my $items_per_worker = int( (@array + $number_of_workers - 1) / $number_of_workers );
    my $items_per_main = @array - $items_per_worker * ($number_of_workers-1);
    my @store_tmp;
    my %child_index; # {pid=>index}
    my $index_worker = $number_of_workers;
    my @result;
    while ($index_worker--) {
        if ($index_worker == 0) {
            # first part - do it yourself
            if ($wantresult) {
                @result = map $code->($_),@array[0..$items_per_main-1];
            } else {
                # ignore the results
                map $code->($_),@array[0..$items_per_main-1];
            }
            last;
        }
        my ($tmp,$key);
        if ($wantresult) {
            $tmp = File::Temp->new(UNLINK=>0);
            $key = $tmp->filename;
        }
        my $pid = fork();
        if ($pid == 0) {
            my ($cur,$next) = map $items_per_main+$items_per_worker * $_, $index_worker-1, $index_worker;
            if ($wantresult) {
                my @result = map $code->($_), @array[$cur..$next-1];
                $store->(\@result,$key);
            } else {
                map $code->($_), @array[$cur..$next-1];
            }
            exit;            
        }
        if ($wantresult) {
            $store_tmp[$index_worker-1] = $tmp;
            $child_index{$pid} = $index_worker-1;
        }
    }
    # now merge kids and their results
    my @results;
    while ((my $pid = wait) != -1) {
        next unless $wantresult;
        my $i = $child_index{$pid};
        my $tmp = $store_tmp[$i];
        my $key = $tmp->filename;
        $results[$i] = $retrieve->($key);
        $tmp->unlink1($key);
    }
    return undef unless $wantresult;
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
