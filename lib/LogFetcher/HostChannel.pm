package LogFetcher::HostChannel;
use Mojo::Base -base;
use Mojo::IOLoop::ReadWriteFork;
use POSIX qw(strftime);
use File::Path qw(make_path);
use Time::HiRes qw(gettimeofday);
use Scalar::Util;


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

=head2 gCfg

GENERAL section form the config file

=cut

has 'gCfg';

has stats => sub {
    return {
        filesTransfered => 0,
        bytesTransfered => 0,
        filesChecked => 0,
    };
};

my @defaultSshOpts = qw(-T -x -y -o BatchMode=yes -o ConnectTimeout=10);

=head1 METHODS

=head2 fetch

go through all the globs on the remote host and see which ones we already go here and
fetch the rest.

=cut

sub makePath {
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
}

sub checkTimeStamp {
    my $self = shift;
    my $src = shift;
    my $timeStamp = shift;
    my $endCheck = shift;
    my $abort = shift;
    # if the fork handle goes out of scope, the fork gets destoryed ...
    # by saving it into this hash we keep it alive until it is done with its work
    state %rc;
    my $checkFork = Mojo::IOLoop::ReadWriteFork->new;
    $rc{$checkFork} = $checkFork;
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

    my $timeout = Mojo::IOLoop->timer($self->gCfg->{timeout} => sub {
        $checkFork->kill(9);
        $abort->("stamp check $src: TIMEOUT");
    });

    $checkFork->on(close => sub {
        Mojo::IOLoop->remove($timeout);
        my $checkFork = shift;
        my $exitValue = shift;
        my $signal = shift;
        delete $rc{$checkFork};
#        if (rand() > 0.5){
#            $abort->("timestamp check $src: random abort");
#            return;
#        }
        if ($exitValue != 0 or $signal){
            $abort->("timestamp check $src: SSH problem Signal $signal, ExitValue $exitValue: ".($firstRead//'no data'));
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
        Mojo::IOLoop->remove($timeout);
        my $checkFork = shift;
        my $error = shift;
        delete $rc{$checkFork};
        $checkFork->kill(9);
        $abort->("stamp check $src: $error");
    });
    my $cmd = "stat --format='<%Y>' $src";
    my @sshArgs = (@{$self->sshConnect},@defaultSshOpts,$cmd);
    $self->log->debug($self->name.': ssh '.join(' ',@sshArgs));
    $checkFork->start(
        program => 'ssh',
        program_args => \@sshArgs,
    );

};

sub checkSumWorking {
    my $self = shift;
    my $working = shift;
    my $endTransfer = shift;
    my $abort = shift;
    # if the fork handle goes out of scope, the fork gets destoryed ...
    # by saving it into this hash we keep it alive until it is done with its work
    state %rc;
    my $checkFork = Mojo::IOLoop::ReadWriteFork->new;
    # save a copy
    $rc{$checkFork} = $checkFork;
    my $timeout = Mojo::IOLoop->timer(600 => sub {
        $checkFork->kill(9);
        $abort->("checksuming check $working: TIMEOUT");
    });

    $checkFork->on(close => sub {
        Mojo::IOLoop->remove($timeout);
        my $checkFork = shift;
        my $exitValue = shift;
        my $signal = shift;
        if ($exitValue == 0){
            $self->log->debug($self->name.": gunzip checksum $working - OK");
        }
        else {
            $abort->("checksum check for $working failed Signal: $signal, ExitValue $exitValue");
        }
        $endTransfer->();
        delete $rc{$checkFork};
    });
    $checkFork->on(error => sub {
        my $checkFork = shift;
        my $error = shift;
        Mojo::IOLoop->remove($timeout);
        $checkFork->kill(9);
        delete $rc{$checkFork};
        $abort->("checksum check for $working: $error");
    });
    my @gunzipArgs = (qw(--test --quiet),$working);
    $self->log->debug($self->name.': gunzip checksum test '.join(' ',@gunzipArgs));
    $checkFork->start(
        program => 'gunzip',
        program_args => \@gunzipArgs,
    );
};

# track the active transfers
my %transferTrack;
my %doneFiles;
my %workFiles;

sub transferFile {
    my $self = shift;
    my $src = shift;
    my $dest = shift;
    my $timeStamp = shift;
    # if the transferFork handle goes out of scope, the fork gets destoryed ...
    # by saving it into this hash we keep it alive until it is done with its work
    state %rc;
    my $transferStarted = 0;
    my $working = $dest.'.working';
    $self->makePath($working);
    my $outLock;
    my $outWrite;
    if (not $workFiles{$working} and open($outWrite, '>', $working)){
        $workFiles{$working} = 1 ;
        my $transferFork = Mojo::IOLoop::ReadWriteFork->new;
        # stringify outside the callback to not
        # create a closure
        my $forkKey = "$transferFork";
        # the keep alive copy
        $rc{$forkKey} = $transferFork;
        my $delay = Mojo::IOLoop->delay(sub {
            my $delay = shift;
            if (my $error = $delay->data('error')){
                $self->log->error($self->name.": fetch $src $dest - $error");
                unlink $working;
                delete $workFiles{$working};
            }
            else {
                rename $working,$dest;
                $doneFiles{$dest} = 1;
                $self->log->info($self->name.": fetch $src $dest ".$delay->data('perfData'));
                $self->stats->{filesTransfered}++;
            }
            # now it can go ... bye bye
            delete $rc{$forkKey};
        });

        my $endTransfer = $delay->begin();
        my $endCheck = $delay->begin();
        Scalar::Util::weaken(my $wtf = $transferFork);
        my $abort = sub {
            my $error = shift;
            $delay->data('error',$error);
            $wtf->kill(9) if $wtf;
            $endTransfer->();
            $endCheck->();
         };

        my $timeoutHandler = sub {
            $abort->("TIMEOUT");
        };

        my $timeoutId = Mojo::IOLoop->timer($self->gCfg->{timeout} => $timeoutHandler);
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
                $self->checkTimeStamp($src,$timeStamp,$endCheck,$abort);
            }
            Mojo::IOLoop->remove($timeoutId);
            $timeoutId = Mojo::IOLoop->timer($self->gCfg->{timeout} => $timeoutHandler);
            my $size = length($chunk);
            $self->stats->{bytesTransfered} += $size;
            $totalSize += $size;
            syswrite $outWrite,$chunk;
        });
        $transferFork->on(close => sub {
            my $transferFork = shift;
            my $exitValue = shift;
            my $signal = shift;
            delete $transferTrack{"$transferFork"};
            Mojo::IOLoop->remove($timeoutId);
            if ($signal or $exitValue or not $transferStarted){
                if (not $delay->data('error')){
                    if ($signal){
                        $delay->data('error',"aborted: Signal $signal");
                    }
                    else {
                        $delay->data('error',"failed: ExitValue $exitValue: ".($firstRead//'no error info'));
                    }
                }
                $endTransfer->();
                $endCheck->();
            }
            else {
                $delay->data('perfData',sprintf("%.1f MB @ %.1f MB/s",
                    $totalSize/1024/1024,$totalSize/1024/1024/(gettimeofday()-$startTime)));
                $self->checkSumWorking($working,$endTransfer,$abort);
            }
        });
        $transferFork->on(error => sub {
            my $transferFork = shift;
            my $error = shift;
            delete $transferTrack{"$transferFork"};
            $delay->data('error',"fetch $src $dest failed: $error");
            Mojo::IOLoop->remove($timeoutId);
            $endTransfer->();
            $endCheck->();
        });
        # no use double compressing things ...
        my $cmd = ($src =~ m/\.gz$/ ? 'cat ' : 'gzip -c ').$src;
        my @sshArgs = (@{$self->sshConnect},@defaultSshOpts,$cmd);
        $self->log->debug($self->name.': ssh '.join(' ',@sshArgs));
        $transferFork->start(
            program => 'ssh',
            program_args => \@sshArgs,
            conduit => 'pipe',
        );
        $transferTrack{"$transferFork"} = 1;
    }
    else {
        $self->log->warn($self->name.": fetch $src $working failed: $!.");
    }
};

# open a new fork
has 'lastLogInfoLine';
has 'hostChannelFirstRead';
has 'hostChannel';

sub makeHostChannel {
    my $self = shift;
    my $controlFork = Mojo::IOLoop::ReadWriteFork->new;
    my $read = '';
    my $firstRead;
    my $taskLimit = $self->gCfg->{transferTaskLimit};
    $self->hostChannelFirstRead('no data');
    $self->lastLogInfoLine(time);
    $controlFork->on(read => sub {
        my $controlFork = shift;
        my $chunk = shift;
        $read .= $chunk;

        while ( $read =~ s/^(.*?)<LOG_FILE><(\d+)><(\d+)><(.+?)><NL>//s ){
            if (not $firstRead){
                $firstRead = $1;
                $self->hostChannelFirstRead($firstRead);
            }
            my ($id,$time,$file) = ($2,$3,$4);
            my $filter = $self->logFiles->[$id]{filterRegexp};
            next if $filter and $file !~ $filter;
            my %match = (
                RXMATCH_1 => $1,
                RXMATCH_2 => $2,
                RXMATCH_3 => $3,
                RXMATCH_4 => $4,
                RXMATCH_5 => $5,
            );
            my $dest = strftime($self->logFiles->[$id]{destinationFile},localtime($time));
            $dest =~ s/\$\{(RXMATCH_[1-5])\}/$match{$1}/g;
            $self->stats->{filesChecked}++;
            $self->lastLogInfoLine(time);
            if (not $doneFiles{$dest} and not -f $dest){
                next if $taskLimit and $taskLimit < scalar keys %transferTrack;
                $self->transferFile($file,$dest,$time);
            }
        }
    });

    $controlFork->on(close => sub {
        my $controlFork = shift;
        my $exitValue = shift;
        my $signal = shift;
        if ($exitValue != 0 and not $signal){
            $self->log->error($self->name.": Host Channel SSH Problem ExitValue $exitValue: ".($firstRead//'no error info'));
        }
        else {
            $self->log->error($self->name.": Host Channel Closed: Signal $signal");
        }
        $self->hostChannel(undef);
    });

    $controlFork->on(error => sub {
        my $controlFork = shift;
        my $error = shift;
        $self->log->error($self->name.': Host Channel Closed - '.$error);
        $self->hostChannel(undef);
    });

    $self->log->debug($self->name.': ssh '.join(' ',@{$self->sshConnect}).' (hostChannel)');
    $controlFork->start(
        program => 'ssh',
        program_args => [@{$self->sshConnect},@defaultSshOpts],
        conduit => 'pipe'
    );
    return $controlFork;
};

sub fetch {
    my $self = shift;
    if (not $self->hostChannel){
        my $hc = $self->makeHostChannel;
        if ($hc){
            $self->hostChannel($hc);
        }
        else {
            return;
        }
    }
    if (time - $self->lastLogInfoLine > $self->gCfg->{timeout}+$self->gCfg->{logCheckInterval}){
        $self->log->error($self->name.': hostChannel not reacting anymore ... lets get a new one. ('.$self->hostChannelFirstRead.')');
        $self->hostChannel->kill(9);
        return;
    };
    my $logFiles = $self->logFiles;
    for (my $id = 0;$id < scalar @$logFiles;$id++){
        $self->hostChannel->write("stat --format='<LOG_FILE><$id><%Y><%n><NL>' "
            . $logFiles->[$id]{globPattern}
            . "\n"
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
