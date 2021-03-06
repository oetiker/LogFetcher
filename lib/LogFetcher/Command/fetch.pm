package LogFetcher::Command::fetch;
use Mojo::Base 'Mojolicious::Command';
use Getopt::Long 2.25 qw(:config posix_default no_ignore_case);
use LogFetcher::HostChannel;

=head1 NAME

LogFetcher::Command::fetch - Log File Syncer

=head1 SYNOPSIS

 ./logfetcher.pl fetch

=head1 DESCRIPTION

Run the logfetcher

=cut

has description => <<'EOF';
copy rotating logfiles from a remote machine to a local archive
EOF

has usage => <<"EOF";
usage: $0 fetch [OPTIONS]

These options are available:

  --verbose   talk about your work

EOF

my %opt;

has log => sub { shift->app->log };
has cfg => sub { shift->app->config->cfgHash };


sub run {
    my $self   = shift;
    local @ARGV = @_ if scalar @_;
    GetOptions(\%opt, 'verbose|v');
    if ($opt{verbose}){
        $ENV{MOJO_LOG_LEVEL} = 'debug';
        $self->log->level('debug');
        $self->log->handle(\*STDOUT);
    }
    my $app = $self->app;


    # setup our tasks
    for my $host (@{$self->cfg->{HOSTS}}){
        # since the $channel is used in the recurring callbacks
        # it does not get DESTORYED prematurely (or every actyally)
        # but here this is fine
        my $channel = LogFetcher::HostChannel->new(%$host,log=>$self->log,gCfg=>$self->cfg->{GENERAL});
        Mojo::IOLoop->recurring( $self->cfg->{GENERAL}{logCheckInterval} => sub {
            $channel->fetch;
        });
        Mojo::IOLoop->recurring( $self->cfg->{GENERAL}{statusLogInterval} => sub {
            my $s = $channel->stats;
            $self->log->info($channel->name.": "
                . "$s->{filesChecked} files checked, "
                . "$s->{filesTransfered} files transfered, "
                . "$s->{bytesTransfered} bytes transfered"
            );
            for (qw(filesChecked filesTransfered bytesTransfered)) {
                $s->{$_} = 0;
            }
        });
        $channel->fetch();
    }

    Mojo::IOLoop->start;
    return $self;
}

1;
__END__

=back

=head1 COPYRIGHT

Copyright (c) 2015 by OETIKER+PARTNER AG. All rights reserved.

=head1 LICENSE

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.

=head1 AUTHOR

S<Tobias Oetiker E<lt>tobi@oetiker.chE<gt>>

=head1 HISTORY

 2015-05-30 to 1.0 first version

=cut

# Emacs Configuration
#
# Local Variables:
# mode: cperl
# eval: (cperl-set-style "PerlStyle")
# mode: flyspell
# mode: flyspell-prog
# End:
#
# vi: sw=4 et
