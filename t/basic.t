use v5.26.0;
use warnings;

use Quantiles;

use Test::Deep;
use Test::More;

my $NOW = 0;
sub elapse { $NOW += $_[0] }

package TestQuantiles {
  use parent 'Quantiles';

  sub now { $NOW }
}

subtest "the simplest quantile checks" => sub {
  my $q = TestQuantiles->new;
  $q->record($_) for 1 .. 100;

  cmp_deeply(
    $q->quantile_summary,
    superhashof({
      sum   => 5050,
      count => 100,
      quantile => {
        50 =>  51,
        90 =>  91,
        95 =>  96,
        99 => 100,
      },
    }),
    "sum and count are as expected",
  );
};


subtest "baic data gathering over time" => sub {
  $NOW = 0;


  # Remember:  default window count = 10, length = 10
  my $q = TestQuantiles->new;

  # After this runs, our windows should look like this:
  # t =  0, (1 .. 100,  2 ..  200)
  # t = 10, (3 .. 300,  4 ..  400)
  # t = 20, (5 .. 500,  6 ..  600)
  # t = 30, (7 .. 700,  8 ..  800)
  # t = 40, (9 .. 900, 10 .. 1000)
  for my $i (1 .. 10) {
    $q->record($_ * $i) for (1 .. 100);
    elapse(5);
  }

  {
    my $v = $q->all_live_values;
    is(@$v, 1000, "all 100 values after 50s!");
  }

  elapse(49);

  {
    my $v = $q->all_live_values;
    is(@$v, 1000, "all 100 values remain after 99s");
  }

  elapse(1);

  {
    my $v = $q->all_live_values;
    is(@$v, 800, "after 100s, prune back to 800 values!");
  }

  my $summary = $q->quantile_summary;

  cmp_deeply(
    $summary,
    superhashof({
      sum   => 262600,
      count => 800,
    }),
    "sum and count are as expected",
  );
};

done_testing;
