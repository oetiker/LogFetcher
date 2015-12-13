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

=head2 hostChannel

a L<Mojo::IOLoop::ReadWriteFork> instance connected to the remote host

=cut

has 'hostChannel';

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

my $checkTimeStamp = sub {
    state %forkCache;
    my $self = shift;
    my $src = shift;
    my $timeStamp = shift;
    my $endCheck = shift;
    my $abort = shift;
    my $checkFork = Mojo::IOLoop::ReadWriteFork->new;
    $forkCache{"$checkFork"} = $checkFork;
    my $remoteTimeStamp = 99;
    my $read = '';

    $checkFork->on(read => sub {
        my $checkFork = shift;
        $read .= shift;
        if ($read =~ /<(\d+)>/){
            $remoteTimeStamp = $1;
        }
    });
    my $timeout = Mojo::IOLoop->timer(5 => sub {
        $checkFork->kill();
        delete $forkCache{"$checkFork"};
        $abort->("stamp check $src: TIMEOUT");
    });

    $checkFork->on(close => sub {
        Mojo::IOLoop->remove($timeout);
        my $checkFork = shift;
        delete $forkCache{"$checkFork"};
        if ($remoteTimeStamp == $timeStamp){
            $self->log->debug("timestamp check $src: verification ok");
            $endCheck->();
        }
        else {
            $abort->("timestamp check $src: timestamp is $remoteTimeStamp and not $timeStamp as expected.");
        }
    });
    $checkFork->on(error => sub {
        my $checkFork = shift;
        my $error = shift;
        delete $forkCache{"$checkFork"};
        $abort->("stamp check $src: $error");
    });
    my $cmd = "stat --format='<%Y>' $src";
    my @sshArgs = (@{$self->sshConnect},qw(-T -x -y),$cmd);
    $self->log->debug('ssh '.join(' ',@sshArgs));
    $checkFork->start(
        program => 'ssh',
        program_args => \@sshArgs,
    );

};

my $transferFile = sub {
    state %taskCache;
    my $self = shift;
    my $src = shift;
    my $dest = shift;
    my $timeStamp = shift;
    my $transferStarted = 0;
    my $working = $dest.'.working';
    $self->$makePath($working);
    my $out;
    if (open $out, '>>', $working and flock($out,LOCK_EX)){
        my $transferFork = Mojo::IOLoop::ReadWriteFork->new;
        $taskCache{"$transferFork"} = $transferFork;

        my $delay = Mojo::IOLoop->delay(sub {
            my $delay = shift;
            my $ok = ( not $delay->data('error')
                and $delay->data('exitValue') == 0
                and not $delay->data('signal'));
            if ($ok){
                rename $working,$dest;
                $self->log->debug("fetch $src $dest SUCCESS");
                unlink $working;
            }
            else {
                $self->log->error("fetch $src $dest FAILED");
            }
            flock($out, LOCK_UN);
            close($out);
            # the fork is not needed anymore it can be destroyed now
            delete $taskCache{"$transferFork"};
            delete $taskCache{"$delay"};
        });
        $taskCache{"$delay"} = $delay;

        my $endTransfer = $delay->begin();
        my $endCheck = $delay->begin();

        my $abort = sub {
                my $error = shift;
                $delay->data('error',$error);
                $transferFork->kill;
                delete $taskCache{"$transferFork"};
                delete $taskCache{"$delay"};
                unlink $working;
                $self->log->error("$error");
        };

        my $timeoutHandler = sub {
            $abort->("fetch $src $dest: TIMEOUT");
        };
        my $timeoutId = Mojo::IOLoop->timer(5 => $timeoutHandler);
        $transferFork->on(read => sub {
            my $transferFork = shift;
            my $chunk = shift;
            if (not $transferStarted){
                $self->log->debug("fetch $src $dest first byte");
                $transferStarted = 1;
                $self->$checkTimeStamp($src,$timeStamp,$endCheck,$abort);
            }
            Mojo::IOLoop->remove($timeoutId);
            $timeoutId = Mojo::IOLoop->timer(5 => $timeoutHandler);
            syswrite $out,$chunk;
        });
        $transferFork->on(close => sub {
            my $transferFork = shift;
            my $exitValue = shift;
            my $signal = shift;
            $delay->data(exitValue => $exitValue);
            $delay->data(signal => $signal);
            $endTransfer->();
        });
        $transferFork->on(error => sub {
            my $transferFork = shift;
            my $error = shift;
            $self->log->error("fetch $src $dest: $error");
            delete $taskCache{"$transferFork"};
            $abort->();
        });
        my $cmd = 'gzip -c '.$src;
        my @sshArgs = (@{$self->sshConnect},qw(-T -x -y),$cmd);
        $self->log->debug('ssh '.join(' ',@sshArgs));
        $transferFork->start(
            program => 'ssh',
            program_args => \@sshArgs,
            conduit => 'pipe',
        );

    }
    else {
        $self->log->warn("fetch $working already in progress. skipping");
    }
};

# open a new fork
has waitingForStat => 0;

my $makeHostChannel;
$makeHostChannel = sub {
    my $self = shift;
    my $controlFork = Mojo::IOLoop::ReadWriteFork->new;
    my $read;
    $controlFork->on(read => sub {
        $read .= $_[1];
        while ( $read =~ s/^.*?<LOG_FILE><(\d+)><(\d+)><(.+?)><NL>//s ){
            $self->waitingForStat(0);
            my ($id,$time,$file) = ($1,$2,$3);
            my $filter = $self->logFiles->[$id]{filterRegexp};
            next if $filter and $file !~ $filter;
            my $dest = strftime($self->logFiles->[$id]{destinationFile},localtime($time));
            if (not -f $dest){
                $self->$transferFile($file,$dest,$time);
            }
        }
    });
    $controlFork->on(close => sub {
        $self->log->error('Host Channel Closed');
        $self->hostChannel($self->$makeHostChannel());
    });
    $controlFork->on(error => sub {
        $self->log->error('Host Channel Closed');
        $self->hostChannel($self->$makeHostChannel());
    });
    $controlFork->start(
        program => 'ssh',
        program_args => $self->sshConnect,
        conduit => 'pty'
    );

    return $controlFork;
};

has hostChannel => sub {
    shift->$makeHostChannel();
};

sub fetch {
    my $self = shift;
    my $logFiles = $self->logFiles;
    for (my $id = 0;$id < scalar @$logFiles;$id++){
        $self->hostChannel->write("stat --format='<LOG_FILE><$id><%Y><%n><NL>' "
            . $logFiles->[$id]{globPattern}
            ."\n"
        );
    }
    $self->waitingForStat(time);
    Mojo::IOLoop->timer(5 => sub {
        if ($self->waitingForStat){
            $self->log->error('hostChannel not reacting anymore ... lets get a new one.');
            $self->hostChannel->kill();
            $self->hostChannel($self->$makeHostChannel());
        };
    });
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
