use Benchmark;
use lib 'lib';
use Parallel::parallel_map;
sub f {
  my ($m) = @_;
  my $r = 1;
  $r *= $m, $r %= 10000 for 1..10000;
}
timethese(-60, {
    'parallel_map' => sub { parallel_map {f($_)} 1..10000 },
    'map' => sub { map {f($_)} 1..10000 },
});
  