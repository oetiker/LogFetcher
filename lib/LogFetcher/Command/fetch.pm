package LogFetcher::Command::fetch;
use Mojo::Base 'Mojolicious::Command';
use Getopt::Long 2.25 qw(:config posix_default no_ignore_case);
use LogFetcher::HostChannel;
use Devel::Cycle;
=head1 NAME

LogFetcher::Command::sync - Ptp2Fibu3 syncer

=head1 SYNOPSIS

 ./logfetcher.pl fetch

=head1 DESCRIPTION

Run the fibu3 syncer

=cut

has description => <<'EOF';
copy rotating logfiles from a remote machine to a local archive
EOF

has usage => <<"EOF";
usage: $0 fetch [OPTIONS]

These options are available:

  --verbose   talk about your work
  --noaction  do not actually write to the rrd files

EOF

my %opt;

has log => sub { shift->app->log };
has cfg => sub { shift->app->config->cfgHash };


sub run {
    my $self   = shift;
    local @ARGV = @_ if @_;
    GetOptions(\%opt, 'daemon|d', 'noaction|no-action|n', 'verbose|v');
    if ($opt{verbose}){
        $self->log->level('debug');
        $self->app->log->handle(\*STDOUT);
    }
    my $app = $self->app;
    my @c;
    for my $host (@{$self->cfg->{HOSTS}}){
        my $channel = LogFetcher::HostChannel->new(%$host,log=>$self->log,gCfg=>$self->cfg->{GENERAL});
        Mojo::IOLoop->recurring( $self->cfg->{GENERAL}{logCheckInterval} => sub {
            # $self->log->debug('check for new logfiles');
            $channel->fetch;
            #print "#################\n";
            #find_cycle($channel);
            #print "#################\n";
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
        push @c,$channel;
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

 2011-05-30 to 1.0 first version

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
