package LogFetcher::HostChannel;
use Mojo::Base -base;
use Mojo::IOLoop::ReadWriteFork;
use POSIX qw(strftime);
use File::Path qw(make_path);
 use Fcntl qw(:flock);

=head1 NAME

LogFetcher::HostChannel - host channel

=head1 SYNOPSIS

    my $cc = LogFetcher::HostChannel->new(sshArgs=>[ssh arguments],logFiles=>[log files]);
    $cc->fetch();

=head1 DESCRIPTION

Open an ssh connection with the given arguments. Then figure out the modification times for all
matching files on the remote system and map them to their local storage location. If the destination file
already exists, skip. Otherwhise launch a separate ssh process to download the file and store it locally.

=cut

=head1 ATTRIBUTES

=head2 sshConnect

argument array to pass on to ssh to open a connection to a remote host

=cut

has sshConnect => sub {
    return [];
};

=head2 logFiles

a list of logfile hashes as defined in the config format. These are the files we are going to watch.

=cut

has logFiles => sub {
    return [];
};

=head2 log

a L<Mojo::Log> object

=cut

has 'log';

=head2 hostControl

a L<Mojo::IOLoop::ReadWriteFork> instance connected to the remote host

=cut

has 'hostControl';

=head1 METHODS

=head2 fetch

go through all the globs on the remote host and see which ones we already go here and
fetch the rest.

=cut

my $makePath = sub {
    my $self = shift;
    my $working = shift;
    if ($working =~ m{^(/.+/)} and not -d $1){
        make_path($1,{error => \my $err});
        if (@$err) {
            for my $diag (@$err) {
                my ($file, $message) = %$diag;
                if ($file eq '') {
                    $self->log->error("mkdir $working: $message");
                }
                else {
                    $self->log->error("problem creating $file: $message");
                }
            }
            return;
        }
    }
};

my $fetchFile = sub {
    state %forkCache;
    my $self = shift;
    my $src = shift;
    my $dest = shift;
    my $started = 0;
    my $working = $dest.'.working';
    $self->$makePath($working);
    my $out;
    if (open $out, '>>', $working and flock($out,LOCK_EX)){
        my $fork = Mojo::IOLoop::ReadWriteFork->new;
        $fork->on(read => sub {
            my $fork = shift;
            my $chunk = shift;
            if (not $started){
                $self->log->debug("fetch $src $dest first byte");
                $started = 1;
            }
            syswrite $out,$chunk;
        });
        $fork->on(close => sub {
            my $fork = shift;
            my $exitValue = shift;
            my $signal = shift;
            if ($exitValue == 0 and not $signal){
                rename $working,$dest;
                $self->log->debug("fetch $src $dest complete");
            }
            else {
                $self->log->error("fetch $src $dest failed with exit code $exitValue");
                unlink $working;
            }
            flock($out, LOCK_UN);
            close($out);
            delete $forkCache{"$fork"};
        });
        $fork->on(error => sub {
            my $fork = shift;
            my $error = shift;
            $self->log->error("fetch $src $dest: $error");
            unlink $dest.'.working';
            flock($out, LOCK_UN);
            close($out);
            delete $forkCache{"$fork"};
        });
        my $cmd = 'gzip -c '.$src;
        $self->log->info('Open SSH Channel: '.join(' ',@{$self->sshConnect},qw(-T -x -y),$cmd));
        $fork->start(
            program => 'ssh',
            program_args => [@{$self->sshConnect},qw(-T -x -y),$cmd],
            conduit => 'pipe'
        );
        $forkCache{"$fork"} = $fork;
    }
    else {
        $self->log->warn("fetch $working already in progress. skipping");
    }
};

# open a new fork
my $makeHostControl = sub {
    my $self = shift;
    my $fork = Mojo::IOLoop::ReadWriteFork->new;
    my $read;
    my @forks;
    $fork->on(read => sub {
        $read .= $_[1];
        while ( $read =~ s/^.*?<LOG_FILE><(\d+)><(\d+)><(.+?)><NL>//s ){
            my ($id,$time,$file) = ($1,$2,$3);
            my $filter = $self->logFiles->[$id]{filterRegexp};
            next if $filter and $file !~ $filter;
            my $dest = strftime($self->logFiles->[$id]{destinationFile},localtime($time));
            if (not -f $dest){
                push @forks,$self->$fetchFile($file,$dest);
            }
        }
    });
    $fork->on(close => sub {
        $self->hostControl(undef);
    });
    $fork->on(error => sub {
        $self->hostControl(undef);
    });
    $fork->start(
        program => 'ssh',
        program_args => $self->sshConnect,
        conduit => 'pty'
    );
    return $fork;
};



sub fetch {
    my $self = shift;
    if (not $self->hostControl){
        $self->log->info('Open SSH Channel: '.join(' ',@{$self->sshConnect}));
        $self->hostControl($self->$makeHostControl());
    }
    my $logFiles = $self->logFiles;
    for (my $id = 0;$id < scalar @$logFiles;$id++){
        $self->hostControl->write("stat --format='<LOG_FILE><$id><%Y><%n><NL>' "
            . $logFiles->[$id]{globPattern}
            ."\n"
        );
    }
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
