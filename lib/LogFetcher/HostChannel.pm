package LogFetcher::HostChannel;
use Mojo::Base -base;
use Mojo::IOLoop::ReadWriteFork;
use POSIX qw(strftime);
use File::Path qw(make_path);
use Fcntl qw(:flock);
use Time::HiRes qw(gettimeofday);

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

=head2 name

identifier for this channel

=cut

has 'name';

=head2 log

a L<Mojo::Log> object

=cut

has 'log';

=head2 hostChannel

a L<Mojo::IOLoop::ReadWriteFork> instance connected to the remote host

=cut

has 'hostChannel';

=head2 statistics

a hash of statistics

=cut

has stats => sub {
    return {
        filesTransfered => 0,
        bytesTransfered => 0,
        filesChecked => 0,
    };
};

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
                    $self->log->error($self->name.": mkdir $working: $message");
                }
                else {
                    $self->log->error($self->name.": problem creating $file: $message");
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
    my $firstRead;
    $checkFork->on(read => sub {
        my $checkFork = shift;
        my $chunk = shift;
        $read .= $chunk;
        $firstRead //= substr($chunk,1,256);
        if ($read =~ /<(\d+)>/){
            $remoteTimeStamp = $1;
        }
    });
    my $timeout = Mojo::IOLoop->timer(5 => sub {
        $checkFork->kill(9);
        delete $forkCache{"$checkFork"};
        $abort->("stamp check $src: TIMEOUT");
    });

    $checkFork->on(close => sub {
        Mojo::IOLoop->remove($timeout);
        my $checkFork = shift;
        my $exitValue = shift;
        my $signal = shift;
        delete $forkCache{"$checkFork"};
        if ($exitValue != 0 or $signal){
            $abort->("SSH problem Signal $signal, ExitValue $exitValue: ".$firstRead);
            return;
        }
        if ($remoteTimeStamp == $timeStamp){
            $self->log->debug($self->name.": timestamp check $src: verification ok");
            $endCheck->();
        }
        else {
            $abort->("timestamp check $src: timestamp is $remoteTimeStamp and not $timeStamp as expected.");
            return;
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
    $self->log->debug($self->name.': ssh '.join(' ',@sshArgs));
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
            if (not $delay->data('error')){
                rename $working,$dest;
                $self->log->info($self->name.": fetch $src $dest ".$delay->data('perfData'));
                $self->stats->{filesTransfered}++;
                unlink $working;
            }
            else {
                $self->log->error($self->name.": fetch $src $dest FAILED");
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
                delete $taskCache{"$transferFork"};
                delete $taskCache{"$delay"};
                unlink $working;
                $self->log->error($self->name.": $error");
                $transferFork->kill(9);
        };

        my $timeoutHandler = sub {
            $self->log->error($self->name.": fetch $src $dest: TIMEOUT");
            $abort->("fetch $src $dest: TIMEOUT");
        };

        my $timeoutId = Mojo::IOLoop->timer(5 => $timeoutHandler);
        my $startTime;
        my $totalSize = 0;
        my $firstRead;
        $transferFork->on(read => sub {
            my $transferFork = shift;
            my $chunk = shift;
            $firstRead //= substr($chunk,1,256);
            if (not $transferStarted){
                $startTime = gettimeofday();
                # $self->log->debug($self->name.": fetch $src $dest first byte");
                $transferStarted = 1;
                $self->$checkTimeStamp($src,$timeStamp,$endCheck,$abort);
            }
            Mojo::IOLoop->remove($timeoutId);
            $timeoutId = Mojo::IOLoop->timer(5 => $timeoutHandler);
            my $size = length($chunk);
            $self->stats->{bytesTransfered} += $size;
            $totalSize += $size;
            syswrite $out,$chunk;
        });
        $transferFork->on(close => sub {
            my $transferFork = shift;
            my $exitValue = shift;
            my $signal = shift;
            Mojo::IOLoop->remove($timeoutId);
            if ($signal or $exitValue or not $transferStarted){
                $abort->("fetch $src $dest failed: Signal $signal, ExitValue $exitValue: $firstRead");
            }
            else {
                $delay->data('perfData',sprintf("%.1f MB @ %.1f MB/s",
                    $totalSize/1024/1024,$totalSize/1024/1024/(gettimeofday()-$startTime)));
                $endTransfer->();
            }
        });
        $transferFork->on(error => sub {
            my $transferFork = shift;
            my $error = shift;
            $self->log->error($self->name.": fetch $src $dest: $error");
            delete $taskCache{"$transferFork"};
            Mojo::IOLoop->remove($timeoutId);
            $abort->("fetch $src $dest: $error");
        });
        my $cmd = 'gzip -c '.$src;
        my @sshArgs = (@{$self->sshConnect},qw(-T -x -y),$cmd);
        $self->log->debug($self->name.': ssh '.join(' ',@sshArgs));
        $transferFork->start(
            program => 'ssh',
            program_args => \@sshArgs,
            conduit => 'pipe',
        );

    }
    else {
        $self->log->warn($self->name.": fetch $src $working already in progress. skipping");
    }
};

# open a new fork
has waitingForStat => sub { 0 };

my $makeHostChannel;
$makeHostChannel = sub {
    my $self = shift;
    my $controlFork = Mojo::IOLoop::ReadWriteFork->new;
    my $read;
    my $firstRead;
    $controlFork->on(read => sub {
        my $controlFork = shift;
        my $chunk = shift;
        $read .= $chunk;
        $firstRead //= substr($chunk,0,256);
        while ( $read =~ s/^.*?<LOG_FILE><(\d+)><(\d+)><(.+?)><NL>//s ){
            $self->waitingForStat(0);
            my ($id,$time,$file) = ($1,$2,$3);
            my $filter = $self->logFiles->[$id]{filterRegexp};
            next if $filter and $file !~ $filter;
            my $dest = strftime($self->logFiles->[$id]{destinationFile},localtime($time));
            $self->stats->{filesChecked}++;
            if (not -f $dest){
                $self->$transferFile($file,$dest,$time);
            }
        }
    });
    $controlFork->on(close => sub {
        my $controlFork = shift;
        my $exitValue = shift;
        my $signal = shift;
        if ($exitValue != 0 and not $signal){
            $self->log->error($self->name.": SSH problem Signal $signal, ExitValue $exitValue: ".$firstRead);
        }
        else {
            $self->log->error($self->name.": Host Channel Closed: Signal $signal");
        }
        $self->hostChannel($self->$makeHostChannel());
    });
    $controlFork->on(error => sub {
        my $controlFork = shift;
        my $error = shift;
        $self->log->error($self->name.': Host Channel Closed - '.$error);
        $self->hostChannel($self->$makeHostChannel());
    });
    $self->log->debug($self->name.': ssh '.join(' ',@{$self->sshConnect}).' (hostChannel)');
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
            $self->log->error($self->name.': hostChannel not reacting anymore ... lets get a new one.');
            $self->hostChannel->kill(9);
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
