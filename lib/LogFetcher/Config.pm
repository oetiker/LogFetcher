package LogFetcher::Config;

use Mojo::Base -base;
use Mojo::JSON qw(decode_json);
use Mojo::Util qw(slurp);
use Carp qw(croak);
use Mojo::Exception;
use Data::Processor;
use Data::Processor::ValidatorFactory;

=head1 NAME

LogFetcher::Config - the Config access  class

=head1 SYNOPSIS

 use LogFetcher::Config;
 my $conf = LogFetcher::Config->new(app=>$app,file=>'config.json');
 my $hash = $conf->cfgHash;
 print $hash->{GENERAL}{value};

=head1 DESCRIPTION

Load and preprocess a configuration file in json format.

=head1 ATTRIBUTES

All the attributes from L<Mojo::Base> as well as:

=head2 app

pointing to the app

=cut

has 'app';

has validatorFactory => sub {
    Data::Processor::ValidatorFactory->new;
};

has validator => sub {
    Data::Processor->new(shift->schema);
};

=head2 file

the path of the config file

=cut

has 'file';


=head2 SCHEMA

the flattened content of the config file

=cut




use Data::Dumper;

my $CONSTANT_RE = '[_A-Z]+';

has schema => sub {
    my $self = shift;
    my $vf = $self->validatorFactory;
    my $string = $vf->rx('^.*$','expected a string');
    my $integer = $vf->rx('^\d+$','expected an integer');

    return {
        GENERAL => {
            description => 'general settings',
            members => {
                logFile => {
                    validator => $vf->file('>>','writing'),
                    description => 'absolute path to log file',
                },
                logLevel => {
                    validator => $vf->rx('(?:debug|info|warn|error|fatal)','Pick a logLevel of debug, info, warn, error or fatal'),
                    description => 'mojo log level - debug, info, warn, error or fatal'
                },
                logCheckInterval => {
                    description => 'log check interval in seconds',
                    validator => $integer,
                },
                statusLogInterval => {
                    description => 'how often to report the log sync status in seconds',
                    validator => $integer,
                },
            },
        },
        CONSTANTS => {
            description => 'define constants fo be used in globPattern and destinationFile properties.',
            optional => 1,
            members => {
                $CONSTANT_RE => {
                    regex => 1,
                    description => 'value of the constant',
                    validator => $string,
                }
            }

        },
        HOSTS => {
            description => 'where does our data come from.',
            array => 1,
            members => {
                name => {
                    description => 'identifier for this host for logfiles',
                    validator => $string,
                },
                sshConnect => {
                    description => 'ssh arguments',
                    array => 1,
                    validator => $string
                },
                logFiles => {
                    description => 'a map of globs on the remote machine',
                    array => 1,
                    members => {
                        globPattern => {
                            description => 'a glob pattern to find all rotated logfile versions on the remote hosts. you can use ${CONSTANTS}',
                            validator => $string
                        },
                        filterRegexp => {
                            optional => 1,
                            description => 'a regular expression to filter the files found by the globPattern',
                            validator => $string
                        },
                        destinationFile => {
                            description => 'where to store the file you can use ${CONSTANTS} and strftime formatting',
                            validator => $string
                        }
                    }
                }
            }
        },
    };
};

=head2 rawCfg

raw config

=cut

has rawCfg => sub {
    my $self = shift;
    $self->n3kCommon->loadJSONCfg($self->file);
};


=head2 cfgHash

access the config hash

=cut

has cfgHash => sub {
    my $self = shift;
    my $cfg = $self->loadJSONCfg($self->file);
    # we need to set this real early to catch all the info in the logfile.
    $self->app->log->path($cfg->{GENERAL}{logFile});
    $self->app->log->level($cfg->{GENERAL}{logLevel});
    my $validator = $self->validator;
    my $hasErrors;
    my $err = $validator->validate($cfg);
    for ($err->as_array){
        warn "$_\n";
        $hasErrors = 1;
    }
    if (my $const = $cfg->{CONSTANTS}){
        my $CONST_MATCH = join('|',keys %$const);
        for my $host (@{$cfg->{HOSTS}}){
            for my $logFile (@{$host->{logFiles}}){
                for my $key (qw(globPattern destinationFile)){
                    $logFile->{$key} =~ s/\$\{($CONST_MATCH)\}/$const->{$1}/g;
                }
            }
        }
    }
    die "Can't continue with config errors\n" if $hasErrors;
    return $cfg;
};

=head1 METHODS

All the methods of L<Mojo::Base> as well as:

=head2 loadJSONCfg(file)

Load the given config, sending error messages to stdout and igonring /// lines as comments

=cut

sub loadJSONCfg {
    my $self = shift;
    my $file = shift;
    my $json = slurp($file);
    $json =~ s{^\s*//.*}{}gm;
    my $raw_cfg = eval { decode_json($json) };
    if ($@){
        if ($@ =~ /(.+?) at line (\d+), offset (\d+)/){
            my $warning = $1;
            my $line = $2;
            my $offset = $3;
            open my $json, '<', $file;
            my $c =0;
            warn "Reading ".$file."\n";
            warn "$warning\n\n";
            while (<$json>){
                chomp;
                $c++;
                if ($c == $line){
                    warn ">-".('-' x $offset).'.'."  line $line\n";
                    warn "  $_\n";
                    warn ">-".('-' x $offset).'^'."\n";
                }
                elsif ($c+3 > $line and $c-3 < $line){
                    warn "  $_\n";
                }
            }
            warn "\n";
            exit 1;
        }
        else {
            Mojo::Exception->throw("Reading ".$file.': '.$@);
        }
    }
    return $raw_cfg;
}


1;

__END__

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

 2014-12-16 to 0.0 first version

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
