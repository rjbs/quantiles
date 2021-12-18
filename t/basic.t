use v5.26.0;
use warnings;

use Quantiles;

use Test::Deep;
use Test::More;
use List::Util qw(sum0);

my %hundred_sec = (window_length => 10, window_count => 10);

my $NOW = 0;
sub elapse { $NOW += $_[0] }

package TestQuantiles {
  use parent 'Quantiles';

  sub now { $NOW }
}

package TestQuantiles::SharedMem {
  use parent '-norequire', 'Quantiles::SharedMem';

  use experimental qw(signatures);

  use Hash::SharedMem ();
  use Path::Tiny ();

  sub new ($class, $arg) {
    my $tempdir = Path::Tiny->tempdir;
    my $shash  = Hash::SharedMem::shash_open("$tempdir", 'cerw');

    my $self = $class->SUPER::new({
      %$arg,
      shash => $shash,
    });

    $self->{__tempdir} = $tempdir;

    return $self;
  }

  sub now { $NOW }
}

subtest "the simplest quantile checks" => sub {
  $NOW = 0;
  for my $class (qw( TestQuantiles TestQuantiles::SharedMem )) {
    subtest $class => sub {
      my $q = $class->new({
        %hundred_sec,
        name => 'simple-test',
      });
      $q->observe_summary($_) for 1 .. 100;

      my $summary = $q->quantile_summary;

      cmp_deeply(
        $summary,
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
      ) or diag(explain($summary));
    }
  };
};

subtest "basic data gathering over time" => sub {
  $NOW = 0;

  for my $class (qw( TestQuantiles TestQuantiles::SharedMem )) {
    subtest $class => sub {
      # Remember:  default window count = 10, length = 10
      my $q = $class->new({
        %hundred_sec,
        name => 'timed-test',
      });

      # After this runs, our windows should look like this:
      # t =  0, (1 .. 100,  2 ..  200)
      # t = 10, (3 .. 300,  4 ..  400)
      # t = 20, (5 .. 500,  6 ..  600)
      # t = 30, (7 .. 700,  8 ..  800)
      # t = 40, (9 .. 900, 10 .. 1000)
      for my $i (1 .. 10) {
        $q->observe_summary($_ * $i) for (1 .. 100);
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

        is(sum0(@$v), 262600, "live values sum to expected value");

        my $summary = $q->quantile_summary;

        cmp_deeply(
          $summary,
          superhashof({
            sum   => 277750,
            count => 1000,
          }),
          "sum and count are as expected",
        );
      }

      elapse(86400);

      {
        my $v = $q->all_live_values;
        is(@$v, 0, "after 1d1m40s, nothing left alive!");

        my $summary = $q->quantile_summary;

        cmp_deeply(
          $summary,
          superhashof({
            sum   => 277750,
            count => 1000,
          }),
          "sum and count are as expected; nothing expires",
        );
      }
    }
  }
};

done_testing;
