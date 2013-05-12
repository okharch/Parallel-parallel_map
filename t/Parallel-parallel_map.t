# Before 'make install' is performed this script should be runnable with
# 'make test'. After 'make install' it should work as 'perl Parallel-parallel_map.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;

use Test::More tests => 11;
BEGIN { use_ok('Parallel::parallel_map') };

#########################

test2x2();

sub test2x2 {
    print "***Testing school 2x2\n";
    for my $n (4,16,64,256,1024) {
    my @data = 1..$n;
    my @result = parallel_map {$_*2} @data;
    ok(@result == $n,"n*2[$n] length");
    my $expected = join(",",map $_*2,1..$n);
    my $got = join(",",@result);
    is($got,$expected,"n*2[$n] values");
	}
}
