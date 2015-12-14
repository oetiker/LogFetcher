package LogFetcher;

use Mojo::Base 'Mojolicious';
use LogFetcher::Config;
our $VERSION = "0.1.0";

=head1 NAME

AzeOP - the application class

=head1 SYNOPSIS

 use Mojolicious::Commands;
 Mojolicious::Commands->start_app('LogFetcher');

=head1 DESCRIPTION

Configure the mojolicious engine to run our application logic

=cut

=head1 ATTRIBUTES

LogFetcher has all the attributes of L<Mojolicious> plus:

=cut

=head2 config

use our own plugin directory and our own configuration file:

=cut

has config => sub {
    my $app = shift;
    LogFetcher::Config->new(
        app => $app,
        file => $ENV{LOGFETCHER_CFG} || $app->home->rel_file('etc/logfetcher.cfg' )
    );
};


sub startup {
    my $app = shift;
    @{$app->commands->namespaces} = (__PACKAGE__.'::Command');
}

1;

=head1 COPYRIGHT

Copyright (c) 2015 by OETIKER+PARTNER AG. All rights reserved.

=head1 AUTHOR

S<Tobias Oetiker E<lt>tobi@oetiker.chE<gt>>

=cut
