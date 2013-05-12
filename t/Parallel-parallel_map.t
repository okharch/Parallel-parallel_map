# Before 'make install' is performed this script should be runnable with
# 'make test'. After 'make install' it should work as 'perl Parallel-parallel_map.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;

use Test::More tests => 3;
BEGIN { use_ok('Parallel::parallel_map') };

#########################

test2x2();

sub test2x2 {
    print "***Testing school 2x2\n";
    my $n = 1000000;
    my @data = 1..$n;
    my @result = parallel_map {$_*2} @data;
    ok(@result == $n,'2x2 length');
    ok(join(",",@result) eq join(",",map $_*2,1..$n),'2x2 values');    
}
