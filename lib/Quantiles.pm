use v5.26.0;
use warnings;

package Quantiles;

use experimental qw(signatures);

use List::Util qw(sum0);

# will want "window length" and "window count"
sub new ($class, $arg = {}) {
  my $window_length = $arg->{window_length} // 10; # seconds
  my $window_count  = $arg->{window_count}  // 10;

  Carp::confess("window_length must be a positive integer")
    unless $window_length > 0 && int($window_length) == $window_length;

  Carp::confess("window_count must be a positive integer")
    unless $window_count > 0 && int($window_count) == $window_count;

  my $self = {
    window_length => $window_length,
    window_count  => $window_count,

    windows       => [
      map {; +{ t => 0, v => [] } } (0 .. $window_count-1)
    ],
  };

  bless $self, $class;

  return $self;
}

sub window_length { $_[0]{window_length} }
sub window_count  { $_[0]{window_count}  }

# This exists to be replaced in tests. -- rjbs, 2021-12-16
sub now { time }

sub record ($self, $value) {
  # Our data structure is C windows, each covering a window of time L seconds
  # long.
  #
  # When we record a datapoint, we compute T, which is the expected start time
  # for the current window.  Then we compute B, the position in the window ring
  # where we'd expect the window for T.
  #
  # If the window at B has not been initialized for T, we initialize it with
  # t=T and v=[]
  #
  # Afterward, no matter what, we append the new value to v.
  #
  # 0.........  1.........  2.........  3.........  4.........  5.........
  # t = 120     t = 130     t = 140     t = 150     t = 160     t = 170
  # t = 180     t = 190     ...
  #
  # This means that we have a resolution of L, where the total number of
  # seconds reported by the object (after it's begun rotation of windows) is
  # between L*(C-1) and L*C at any given time.
  my $now = $self->now;

  my $length   = $self->window_length;
  my $window_t = $now - $now % $length;
  my $window_b = $window_t / $length % $length;

  $self->append_to_window($window_b, $window_t, $value);

  return;
}

sub append_to_window ($self, $ring_index, $start_time, $value) {
  # In a shared memory version of this, we'll do an atomic read-update-write
  # loop.
  my $window_ref = \($self->{windows}[$ring_index]);

  if ($window_ref->$*->{t} != $start_time) {;
    $$window_ref = { t => $start_time, v => [] };
  }

  push $window_ref->$*->{v}->@*, $value;

  return;
}

sub all_windows_arrayref ($self) {
  # In a shared memory version of this, we'll compute all expected keys, then
  # read and deserialize them.
  $self->{windows}
}

sub _all_live_windows ($self, $now) {
  # window length is 10s
  # window count  is 5
  # now is 713
  # now's window starts at 710
  # that's one window; the there are 4 more, meaning 10 * (5-1) second spanned
  # so we want windows with t >= 670
  my $cutoff = ($now - $now % $self->{window_length})
             - ($self->{window_length} * ($self->{window_count} - 1));

  grep {; $_->{t} >= $cutoff } $self->all_windows_arrayref->@*;
}

sub all_live_values ($self) {
  my @values = map {; $_->{v}->@* } $self->_all_live_windows($self->now);
  return \@values;
}

# nth percentile of a set of values = mean + (Zn * stddev)
my %Z_for = (
  90  => 1.282,
  95  => 1.645,
  99  => 2.326,
);

sub quantile_summary ($self) {
  my $values = $self->all_live_values;

  return { sum => 0, count => 0 } unless @$values;

  my $sum   = sum0(@$values);
  my $count = @$values;
  my $mean  = $sum / $count;

  # This is the standard two-pass algorithm for computing stddev.
  my $sum_sq_diff = sum0(map {; ($_ - $mean) ** 2 } @$values);
  my $stddev      = sqrt($sum_sq_diff / (@$values - 1));

  return {
    sum   => $sum,
    count => $count,
    mean  => $mean,
    stddev => $stddev,
    q => { map {; $_ => $mean + $Z_for{$_} * $stddev } keys %Z_for },
  };
}

package Quantiles::SharedMem {
  use parent '-norequire', 'Quantiles';

  use JSON::XS;

  use experimental 'signatures';

  sub new ($class, $arg) {
    my $self  = $class->SUPER::new($arg);

    $self->{shash} = $arg->{shash};
    $self->{name}  = $arg->{name};

    $self->_initialize;

    return 1;
  }

  # key format: q-name-Wnum-Wcount

  sub _initialize ($self) {
    my $count = $self->window_count;

    my $template = encode_json({ t => 0, v => [] });

    for my $i (0 .. $self->window_count - 1) {
      my $key = join q{-}, 'q', $self->{name}, $i, $count;
      shash_set($self->{shash}, $key, $template);
    }

    return;
  }

  sub append_to_window ($self, $ring_index, $start_time, $value) {
    my $key = join q{-}, 'q', $self->{name}, $ring_index, $self->window_count;

    my ($ov, $nv);

    do {
      $ov = Hash::SharedMem::shash_get($self->{shash}, $key);

      my $window = $ov ? decode_json($ov) : undef;

      unless ($window && $window->{t} == $start_time) {
        $window = { t => $start_time, v => [] };
      }

      push $window->{v}->@*, $value;

      $nv = encode_json($window);
    } until shash_cset($self->{shash}, $key, $ov, $nv);

    return;
  }

  sub all_windows_arrayref ($self) {
    my @windows;
    my $ov;

    for my $i (0 .. $#windows) {
      my $key = join q{-}, 'q', $self->{name}, $i, $self->window_count;
      $ov = Hash::SharedMem::shash_get($self->{shash}, $key);
      push @windows, $ov ? decode_json($ov) : ();
    }

    return \@windows;
  }
}

1;