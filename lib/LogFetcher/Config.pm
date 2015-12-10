package LogFetcher::Config;

use Mojo::Base -base;
use Mojo::JSON qw(decode_json);
use Mojo::Util qw(slurp);
use Carp qw(croak);
use List::Util qw(max);
use Mojo::Exception;
use Data::Processor;
use Data::Processor::ValidatorFactory;
use N3KCommon;

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

has n3kCommon => sub {
    N3KCommon->new;
};

=head2 file

the path of the config file

=cut

has 'file';


=head2 SCHEMA

the flattened content of the config file

=cut




use Data::Dumper;


has schema => sub {
    my $self = shift;
    my $vf = $self->validatorFactory;
    my $string = $vf->rx('^\S+$','expected a string');
    my $integer = $vf->rx('^\d+$','expected an integer');
    my $float = $vf->rx('^\d+(\.\d+)?$','expected a floatingpoint number');
    my $dskey = '[-_a-zA-Z0-9]+';
    my $transformer = $self->n3kCommon->transformer;
    return {
        GENERAL => {
            description => 'general settings',
            members => {
                log_file => {
                    validator => $vf->file('>>','writing'),
                    description => 'absolute path to log file',
                },
                cachedb => {
                    validator => $vf->file('>>','writing'),
                    description => 'absolute path to cache (sqlite) database file',
                },
                history => {
                    default => '1d',
                    description => 'time to keep history in cache database. specify in s m h d',
                    example => '3h',
                    transformer => $transformer->{timespec}(
                        'specify cachedb history retention in seconds or append d,m,h to the number'),
                },
                nodeid => {
                    description => "the node id ... aka hostname"
                },
                mib_path => {
                    optional => 1,
                    description => "an array of directories to scour for mib files",
                    array => 1,
                    validator => $string,
                },
                load_mib => {
                    optional => 1,
                    description => "array of mibs to load (use the mib internal names)",
                    array => 1,
                    validator => $string,
                },
                silo_push_interval => {
                    description => "how frequently to send data to the silo. specify in s m h d",
                    default => '10',
                    example => '1m',
                    transformer => $transformer->{timespec}(
                        'specify silo_push_interval in seconds or append d,m,h to the number'),
                },
                silos => {
                    description => 'silos store collected data',
                    # "members" stands for all "non-internal" fields
                    members => {
                        '.+' => {
                            regex => 1,
                            members => {
                                url => {
                                    validator    =>  $vf->rx(qr{^https?://.*},'expected a http url'),
                                    description => 'url of the silo server. Only https:// allowed',
                                },
                                shared_secret => {
                                    description => 'shared secret to authenticate node',
                                    validator => $string
                                }
                            }
                        }
                    }
                }
            }
        },
        DATASTORE => $self->n3kCommon->dataStoreSchema,
        DATASOURCE => {
            description => 'data sources are stored in a three level hierarchic. When',
            members => {
                $dskey => {
                    regex => 1,
                    members => {
                        $dskey => {
                            regex => 1,
                            members => {
                                $dskey => {
                                    regex => 1,
                                    members => {
                                        datastore => {
                                            description => 'Which data store to use for the results of the probe. The DataStore can be defined locally or on the silo. If the same key is defined at both ends, the silo definition gets preference.',
                                            validator => $string,
                                        },
                                        type => {
                                            description => 'Data source type',
                                            validator => $vf->any(qw(DERIVE COUNTER GAUGE)),
                                        },
                                        step => {
                                            description => 'Interval for running this probe. Make sure the data store you choose is prepared to accept data at this interval.',
                                            default => '10',
                                            example => '10s',
                                            transformer => $transformer->{timespec}(
                                                'specify step in seconds or append d,m,h to the number'),
                                        },
                                        probe_cfg => {
                                            description => 'probe config ... we use a schema provided by the probe to validate this.'
                                        },
                                        probe => {
                                            description => 'Probe Module to load for this section',
                                            transformer => sub {
                                                my ($value,$parent) = @_;
                                                return {
                                                    name => $value,
                                                    obj => $self->loadProbe($value,$parent->{probe_cfg}),
                                                };
                                            },
                                            # a list of values to use when generating documentation
                                            # so that all plug-ins can be loaded artificially
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        GRAPHTEMPLATE => {
            description => 'chart templates for use on the silo in connection with the data from this node',
            members => {
                $dskey => {
                    regex => 1,
                    members => {
                        $dskey => {
                            regex => 1,
                            members => {
                                $dskey => {
                                    regex => 1,
                                    description => 'chart definition',
                                    members => $self->n3kCommon->chartSchema,
                                }
                            }
                        }
                    }
                }
            }
        }
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
    my $cfg = $self->n3kCommon->loadJSONCfg($self->file);
    # we need to set this real early to catch all the info in the logfile.
    $self->app->log->path($cfg->{GENERAL}{log_file});
    my $validator = $self->validator;
    my $hasErrors;
    my $err = $validator->validate($cfg);
    for ($err->as_array){
        warn "$_\n";
        $hasErrors = 1;
    }
    for my $section (qw(DATASOURCE GRAPHTEMPLATE)){
        my %secFlat;
        my $sec = $cfg->{$section};
        for my $k1 (keys %{$sec}){
            for my $k2 (keys %{$sec->{$k1}}){
                for my $k3 (keys %{$sec->{$k1}{$k2}}){
                    $secFlat{"$k1/$k2/$k3"} = $sec->{$k1}{$k2}{$k3};
                }
            }
        }
        $cfg->{$section} = \%secFlat;
    }
    die "Can't continue with config errors\n" if $hasErrors;
    $cfg->{CONFIG_CTIME} = +(stat $self->file)[10];
    return $cfg;
};

=head2 probePath

where should be go looking for probe modules ?

=cut

has probePath => sub {
    ['N3KHarvester::Probe'];
};

=head2 probeInventory

returns a hash with probe names an associated files

=cut

has probeInventory => sub {
    my $self   = shift;
    my $probePath = $self->probePath;
    my %probes;
    for my $path (@INC){
        for my $pPath (@$probePath) {
            my @pDirs = split /::/, $pPath;
            my $fPath = File::Spec->catdir($path, @pDirs, '*.pm');
            for my $file (sort glob($fPath)) {
                my ($volume, $modulePath, $moduleName) = File::Spec->splitpath($file);
                $moduleName =~ s{\.pm$}{};
                $probes{$moduleName} = $pPath.'::'.$moduleName;
                # it seems better to just load them all
                # instead of trying to load 'on demand'
                require $file;
            }
        }
    }
    return \%probes;
};

=head1 METHODS

All the methods of L<Mojo::Base> as well as:

=head2 B<loadProbe>('ProbeModule')

Find the given module in the F<probePath>, load it and create an instance.

=cut

sub loadProbe {
    my $self   = shift;
    my $probe_name = shift;
    my $cfg = shift;
    my $module = $self->probeInventory->{$probe_name} or do {
        $self->log->error("Probe module $probe_name not found");
    };
    no strict 'refs';
    my $probe_obj = "$module"->new(
        app => $self->app,
        rawCfg => $self->rawCfg
    );
    my $validator = Data::Processor->new($probe_obj->schema);
    my $err = $validator->validate($cfg);
    for ($err->as_array){
        die {msg => $_};
    }
    $probe_obj->cfg($cfg);
    return $probe_obj;
}



1;

__END__

=head1 COPYRIGHT

Copyright (c) 2014 by OETIKER+PARTNER AG. All rights reserved.

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
