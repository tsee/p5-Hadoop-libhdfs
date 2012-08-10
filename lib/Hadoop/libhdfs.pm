package Hadoop::libhdfs;
use 5.008005;
use strict;
use warnings;

our $VERSION = '0.01';

require XSLoader;
XSLoader::load('Hadoop::libhdfs', $VERSION);

1;
__END__

=head1 NAME

Hadoop::libhdfs - Perl/XS wrapper of the libhdfs library

=head1 SYNOPSIS

  use Hadoop::libhdfs;
  TODO

=head1 DESCRIPTION

TODO document

=head1 SEE ALSO

=head1 ACKNOWLEDGMENT

This module was originally developed for booking.com.
With approval from booking.com, this module was generalized
and put on CPAN, for which the authors would like to express
their gratitude.

=head1 AUTHOR

Steffen Mueller, E<lt>smueller@cpan.org<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.5 or,
at your option, any later version of Perl 5 you may have available.

=cut
