package Perinci::WebScript::Base;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::ger;

use Mo qw(build default);

has actions => (is=>'rw');
#has completion => (is=>'rw');
#has default_subcommand => (is=>'rw');
#has get_subcommand_from_arg => (is=>'rw', default=>1);
#has auto_abbrev_subcommand => (is=>'rw', default=>1);
has description => (is=>'rw');
#has exit => (is=>'rw', default=>1);
#has formats => (is=>'rw');
#has default_format => (is=>'rw');
has program_name => (is=>'rw');
has riap_version => (is=>'rw', default=>1.1);
has riap_client => (is=>'rw');
has riap_client_args => (is=>'rw');
#has subcommands => (is=>'rw');
has summary => (is=>'rw');
has tags => (is=>'rw');
has url => (is=>'rw');
has log => (is=>'rw', default => 0);
has log_level => (is=>'rw');
has read_config => (is=>'rw', default=>1);
has config_filename => (is=>'rw');
has config_dirs => (
    is=>'rw',
    default => sub {
        require Perinci::CmdLine::Util::Config;
        Perinci::CmdLine::Util::Config::get_default_config_dirs();
    },
);

has cleanser => (
    is => 'rw',
    lazy => 1,
    default => sub {
        require Data::Clean::JSON;
        Data::Clean::JSON->get_cleanser;
    },
);
has use_cleanser => (is=>'rw', default=>1);

has extra_urls_for_version => (is=>'rw');

#has skip_format => (is=>'rw');

has use_utf8 => (
    is=>'rw',
    default => sub {
        $ENV{UTF8} // 0;
    },
);

has default_dry_run => (
    is=>'rw',
    default => 0,
);

sub BUILD {
    my ($self, $args) = @_;

    if (!$self->{riap_client}) {
        require Perinci::Access::Lite;
        my %rcargs = (
            riap_version => $self->{riap_version} // 1.1,
            %{ $self->{riap_client_args} // {} },
        );
        $self->{riap_client} = Perinci::Access::Lite->new(%rcargs);
    }

    if (!$self->{actions}) {
        $self->{actions} = {
            form => {},
            call => {},
        };
    }
}

sub hook_before_run {}

sub hook_before_read_config_file {}

sub hook_after_read_config_file {}

sub hook_before_action {}

sub hook_after_action {}

sub get_meta {
    my ($self, $r, $url) = @_;

    my $res = $self->riap_client->request(meta => $url);
    die $res unless $res->[0] == 200;
    my $meta = $res->[2];
    $r->{meta} = $meta;
    log_trace("[periwebs] Running hook_after_get_meta ...");
    $self->hook_after_get_meta($r);
    $meta;
}

sub _read_config {
    require Perinci::CmdLine::Util::Config;

    my ($self, $r) = @_;

    my $hook_section;
    if ($self->can("hook_config_file_section")) {
        $hook_section = sub {
            my ($section_name, $section_content) = @_;
            $self->hook_config_file_section(
                $r, $section_name, $section_content);
        };
    }

    my $res = Perinci::CmdLine::Util::Config::read_config(
        config_paths     => $r->{config_paths},
        config_filename  => $self->config_filename,
        config_dirs      => $self->config_dirs,
        program_name     => $self->program_name,
        hook_section     => $hook_section,
    );
    die $res unless $res->[0] == 200;
    $r->{config} = $res->[2];
    $r->{read_config_files} = $res->[3]{'func.read_files'};
    $r->{_config_section_read_order} = $res->[3]{'func.section_read_order'}; # we currently don't want to publish this request key

    if ($ENV{LOG_DUMP_CONFIG}) {
        log_trace "config: %s", $r->{config};
        log_trace "read_config_files: %s", $r->{read_config_files};
    }
}

sub _parse_argv1 {
    my ($self, $r) = @_;

    # parse common_opts which potentially sets subcommand
    my @go_spec;
    {
        # one small downside for this is that we cannot do autoabbrev here,
        # because we're not yet specifying all options here.

        require Getopt::Long;
        my $old_go_conf = Getopt::Long::Configure(
            'pass_through', 'no_ignore_case', 'no_auto_abbrev',
            'no_getopt_compat', 'gnu_compat', 'bundling');
        my $co = $self->common_opts // {};
        for my $k (keys %$co) {
            push @go_spec, $co->{$k}{getopt} => sub {
                my ($go, $val) = @_;
                $co->{$k}{handler}->($go, $val, $r);
            };
        }
        #log_trace("\@ARGV before parsing common opts: %s", \@ARGV);
        Getopt::Long::GetOptions(@go_spec);
        Getopt::Long::Configure($old_go_conf);
        #log_trace("\@ARGV after  parsing common opts: %s", \@ARGV);
    }

    # select subcommand and fill subcommand data
    {
        my $scn = $r->{subcommand_name};
        my $scn_from = $r->{subcommand_name_from};
        if (!defined($scn) && defined($self->{default_subcommand})) {
            # get from default_subcommand
            if ($self->get_subcommand_from_arg == 1) {
                $scn = $self->{default_subcommand};
                $scn_from = 'default_subcommand';
            } elsif ($self->get_subcommand_from_arg == 2 && !@ARGV) {
                $scn = $self->{default_subcommand};
                $scn_from = 'default_subcommand';
            }
        }
        if (!defined($scn) && $self->{subcommands} && @ARGV) {
            # get from first command-line arg
            if ($ARGV[0] =~ /\A-/) {
                if ($r->{in_completion}) {
                    $scn = shift @ARGV;
                    $scn_from = 'arg';
                } else {
                    my $suggestion = '';
                    my @similar = __find_similar_go_opts($ARGV[0], \@go_spec);
                    $suggestion = " (perhaps you meant ".
                        join("/", @similar)."?)" if @similar;
                    die [400, "Unknown option: $ARGV[0]".$suggestion];
                }
            } else {
                $scn = shift @ARGV;
                $scn_from = 'arg';
            }
        }

        my $scd;
        if (defined $scn) {
            $scd = $self->get_subcommand_data($scn);
            unless ($r->{in_completion}) {
                unless ($scd) {
                    my $scs = $self->list_subcommands;
                    if ($self->auto_abbrev_subcommand) {
                        # check that subcommand is an unambiguous abbreviation
                        # of an existing subcommand
                        my $num_matches = 0;
                        my $complete_scn;
                        for (keys %$scs) {
                            if (index($_, $scn) == 0) {
                                $num_matches++;
                                $complete_scn = $_;
                                last if $num_matches > 1;
                            }
                        }
                        if ($num_matches == 1) {
                            $scn = $complete_scn;
                            $scd = $self->get_subcommand_data($scn);
                            goto L1;
                        }
                    }
                    # provide suggestion of probably mistyped subcommand to user
                    my @similar =
                        __find_similar_strings($scn, [keys %$scs]);
                    my $suggestion = '';
                    $suggestion = " (perhaps you meant ".
                        join("/", @similar)."?)" if @similar;
                    die [500, "Unknown subcommand: $scn".$suggestion];
                }
            }
        } elsif (!$r->{action} && $self->{subcommands}) {
            # program has subcommands but user doesn't specify any subcommand,
            # or specific action. display help instead.
            $r->{action} = 'help';
            $r->{skip_parse_subcommand_argv} = 1;
        } else {
            $scn = '';
            $scd = {
                url => $self->url,
                summary => $self->summary,
                description => $self->description,
                pass_cmdline_object => $self->pass_cmdline_object,
                tags => $self->tags,
            };
        }
      L1:
        $r->{subcommand_name} = $scn;
        $r->{subcommand_name_from} = $scn_from;
        $r->{subcommand_data} = $scd;
    }

    $r->{_parse_argv1_done} = 1;
}

sub _parse_argv2 {
    require Perinci::CmdLine::Util::Config;

    my ($self, $r) = @_;

    my %args;

    if ($r->{read_env}) {
        my $env_words = $self->_read_env($r);
        unshift @ARGV, @$env_words;
    }

    # parse argv for per-subcommand command-line opts
    if ($r->{skip_parse_subcommand_argv}) {
        return [200, "OK (subcommand options parsing skipped)"];
    } else {
        my $scd = $r->{subcommand_data};
        if ($r->{meta} && !$self->{subcommands}) {
            # we have retrieved meta, no need to get it again
        } else {
            $self->get_meta($r, $scd->{url});
        }

        # first fill in from subcommand specification
        if ($scd->{args}) {
            $args{$_} = $scd->{args}{$_} for keys %{ $scd->{args} };
        }

        # then read from configuration
        if ($r->{read_config}) {

            log_trace("[pericmd] Running hook_before_read_config_file ...");
            $self->hook_before_read_config_file($r);

            $self->_read_config($r) unless $r->{config};

            log_trace("[pericmd] Running hook_after_read_config_file ...");
            $self->hook_after_read_config_file($r);

            my $res = Perinci::CmdLine::Util::Config::get_args_from_config(
                r                  => $r,
                config             => $r->{config},
                args               => \%args,
                program_name       => $self->program_name,
                subcommand_name    => $r->{subcommand_name},
                config_profile     => $r->{config_profile},
                common_opts        => $self->common_opts,
                meta               => $r->{meta},
                meta_is_normalized => 1,
            );
            die $res unless $res->[0] == 200;
            log_trace("[pericmd] args after reading config files: %s",
                         \%args);
            my $found = $res->[3]{'func.found'};
            if (defined($r->{config_profile}) && !$found &&
                    defined($r->{read_config_files}) &&
                        @{$r->{read_config_files}} &&
                            !$r->{ignore_missing_config_profile_section}) {
                return [412, "Profile '$r->{config_profile}' not found ".
                            "in configuration file"];
            }

        }

        # finally get from argv

        # since get_args_from_argv() doesn't pass $r, we need to wrap it
        my $copts = $self->common_opts;
        my %old_handlers;
        for (keys %$copts) {
            my $h = $copts->{$_}{handler};
            $copts->{$_}{handler} = sub {
                my ($go, $val) = @_;
                $h->($go, $val, $r);
            };
            $old_handlers{$_} = $h;
        }

        my $has_cmdline_src;
        for my $ak (keys %{$r->{meta}{args} // {}}) {
            my $av = $r->{meta}{args}{$ak};
            if ($av->{cmdline_src}) {
                $has_cmdline_src = 1;
                last;
            }
        }

        require Perinci::Sub::GetArgs::Argv;
        my $ga_res = Perinci::Sub::GetArgs::Argv::get_args_from_argv(
            argv                => \@ARGV,
            args                => \%args,
            meta                => $r->{meta},
            meta_is_normalized  => 1,
            allow_extra_elems   => $has_cmdline_src ? 1:0,
            per_arg_json        => $self->{per_arg_json},
            per_arg_yaml        => $self->{per_arg_yaml},
            common_opts         => $copts,
            strict              => $r->{in_completion} ? 0:1,
            (ggls_res            => $r->{_ggls_res}) x defined($r->{_ggls_res}),
            on_missing_required_args => sub {
                my %a = @_;

                my ($an, $aa, $as) = ($a{arg}, $a{args}, $a{spec});
                my $src = $as->{cmdline_src} // '';

                # we only get from stdin if stdin is piped
                $src = '' if $src eq 'stdin_or_args' && -t STDIN;

                if ($src && $as->{req}) {
                    # don't complain, we will fill argument from other source
                    return 1;
                } else {
                    # we have no other sources, so we complain about missing arg
                    return 0;
                }
            },
        );

        return $ga_res unless $ga_res->[0] == 200;

        # wrap stream arguments with iterator
        my $args_p = $r->{meta}{args} // {};
        for my $arg (keys %{$ga_res->[2]}) {
            next unless $args_p->{$arg};
            next unless $args_p->{$arg}{stream};
            for ($ga_res->[2]{$arg}) {
                $_ = ref $_ eq 'ARRAY' ? __array_iter($_) : __list_iter($_);
            }
        }

        # restore
        for (keys %$copts) {
            $copts->{$_}{handler} = $old_handlers{$_};
        }

        return $ga_res;
    }
}

sub parse_argv {
    my ($self, $r) = @_;

    log_trace("[pericmd] Parsing \@ARGV: %s", \@ARGV);

    # we parse argv twice. the first parse is with common_opts only so we're
    # able to catch --help, --version, etc early without having to know about
    # subcommands. two reasons for this: sometimes we need to get subcommand
    # name *from* cmdline opts (e.g. --cmd) and thus it's a chicken-and-egg
    # problem. second, it's faster because we don't have to load Riap client and
    # request the meta through it (especially in the case of remote URL).
    #
    # the second parse is after ge get subcommand name and the function
    # metadata. we can parse the remaining argv to get function arguments.
    #
    # note that when doing completion we're not using this algorithem and only
    # parse argv once. this is to make completion work across common- and
    # per-subcommand opts, e.g. --he<tab> resulting in --help (common opt) as
    # well as --height (function argument).

    $self->_parse_argv1($r) unless $r->{_parse_argv1_done};
    $self->_parse_argv2($r);
}

sub __gen_iter {
    require Data::Sah::Util::Type;

    my ($fh, $argspec, $argname) = @_;
    my $schema = $argspec->{schema};
    $schema = $schema->[1]{of} if $schema->[0] eq 'array';
    my $type = Data::Sah::Util::Type::get_type($schema);

    if (Data::Sah::Util::Type::is_simple($schema)) {
        my $chomp = $type eq 'buf' ? 0 :
            $argspec->{'cmdline.chomp'} // 1;
        return sub {
            # XXX this will be configurable later. currently by default reading
            # binary is per-64k while reading string is line-by-line.
            local $/ = \(64*1024) if $type eq 'buf';

            state $eof;
            return undef if $eof;
            my $l = <$fh>;
            unless (defined $l) {
                $eof++; return undef;
            }
            chomp($l) if $chomp;
            $l;
        };
    } else {
        # expect JSON stream for non-simple types
        require JSON::MaybeXS;
        state $json = JSON::MaybeXS->new->allow_nonref;
        my $i = -1;
        return sub {
            state $eof;
            return undef if $eof;
            $i++;
            my $l = <$fh>;
            unless (defined $l) {
                $eof++; return undef;
            }
            eval { $l = $json->decode($l) };
            if ($@) {
                die "Invalid JSON in stream argument '$argname' record #$i: $@";
            }
            $l;
        };
    }
}

# parse cmdline_src argument spec properties for filling argument value from
# file and/or stdin. currently does not support argument submetadata.
sub parse_cmdline_src {
    my ($self, $r) = @_;

    my $action = $r->{action};
    my $meta   = $r->{meta};

    my $url = $r->{subcommand_data}{url} // $self->{url} // '';
    my $is_network = $url =~ m!^(https?|riap[^:]+):!;

    # handle cmdline_src
    if ($action eq 'call') {
        my $args_p = $meta->{args} // {};
        my $stdin_seen;
        for my $an (sort {
            my $csa  = $args_p->{$a}{cmdline_src};
            my $csb  = $args_p->{$b}{cmdline_src};
            my $posa = $args_p->{$a}{pos} // 9999;
            my $posb = $args_p->{$b}{pos} // 9999;

            # first, always put stdin_line before stdin / stdin_or_files
            (
                !$csa || !$csb ? 0 :
                    $csa eq 'stdin_line' && $csb eq 'stdin_line' ? 0 :
                    $csa eq 'stdin_line' && $csb =~ /^(stdin|stdin_or_files?|stdin_or_args)/ ? -1 :
                    $csb eq 'stdin_line' && $csa =~ /^(stdin|stdin_or_files?|stdin_or_args)/ ? 1 : 0
            )
            ||

            # then order by pos
            ($posa <=> $posb)

            ||
            # then by name
            ($a cmp $b)
        } keys %$args_p) {
            #log_trace("TMP: handle cmdline_src for arg=%s", $an);
            my $as = $args_p->{$an};
            my $src = $as->{cmdline_src};
            my $type = $as->{schema}[0]
                or die "BUG: No schema is defined for arg '$an'";
            # Riap::HTTP currently does not support streaming input
            my $do_stream = $as->{stream} && $url !~ /^https?:/;
            if ($src) {
                die [531,
                     "Invalid 'cmdline_src' value for argument '$an': $src"]
                    unless $src =~ /\A(stdin|file|stdin_or_files?|stdin_or_args|stdin_line)\z/;
                die [531,
                     "Sorry, argument '$an' is set cmdline_src=$src, but type ".
                         "is not str/buf/array, only those are supported now"]
                    unless $do_stream || $type =~ /\A(str|buf|array)\z/; # XXX stdin_or_args needs array only, not str/buf

                if ($src =~ /\A(stdin|stdin_or_files?|stdin_or_args)\z/) {
                    die [531, "Only one argument can be specified ".
                             "cmdline_src stdin/stdin_or_file/stdin_or_files/stdin_or_args"]
                        if $stdin_seen++;
                }
                my $is_ary = $type eq 'array';
                if ($src eq 'stdin_line' && !exists($r->{args}{$an})) {
                    require Perinci::Object;
                    my $term_readkey_available = eval { require Term::ReadKey; 1 };
                    my $prompt = Perinci::Object::rimeta($as)->langprop('cmdline_prompt') //
                        sprintf($self->default_prompt_template, $an);
                    print $prompt;
                    my $iactive = (-t STDOUT);
                    Term::ReadKey::ReadMode('noecho')
                          if $term_readkey_available && $iactive && $as->{is_password};
                    chomp($r->{args}{$an} = <STDIN>);
                    do { print "\n"; Term::ReadKey::ReadMode(0) if $term_readkey_available }
                        if $iactive && $as->{is_password};
                    $r->{args}{"-cmdline_src_$an"} = 'stdin_line';
                } elsif ($src eq 'stdin' || $src eq 'file' &&
                        ($r->{args}{$an}//"") eq '-') {
                    die [400, "Argument $an must be set to '-' which means ".
                             "from stdin"]
                        if defined($r->{args}{$an}) &&
                            $r->{args}{$an} ne '-';
                    #log_trace("Getting argument '$an' value from stdin ...");
                    $r->{args}{$an} = $do_stream ?
                        __gen_iter(\*STDIN, $as, $an) :
                            $is_ary ? [<STDIN>] :
                                do {local $/; ~~<STDIN>};
                    $r->{args}{"-cmdline_src_$an"} = 'stdin';
                } elsif ($src eq 'stdin_or_file' || $src eq 'stdin_or_files') {
                    # push back argument value to @ARGV so <> can work to slurp
                    # all the specified files
                    local @ARGV = @ARGV;
                    unshift @ARGV, $r->{args}{$an}
                        if defined $r->{args}{$an};

                    # with stdin_or_file, we only accept one file
                    splice @ARGV, 1
                        if @ARGV > 1 && $src eq 'stdin_or_file';

                    #log_trace("Getting argument '$an' value from ".
                    #                 "$src, \@ARGV=%s ...", \@ARGV);

                    # perl doesn't seem to check files, so we check it here
                    for (@ARGV) {
                        next if $_ eq '-';
                        die [500, "Can't read file '$_': $!"] if !(-r $_);
                    }

                    $r->{args}{"-cmdline_srcfilenames_$an"} = [@ARGV];
                    $r->{args}{$an} = $do_stream ?
                        __gen_iter(\*ARGV, $as, $an) :
                            $is_ary ? [<>] :
                                do {local $/; ~~<>};
                    $r->{args}{"-cmdline_src_$an"} = $src;
                } elsif ($src eq 'stdin_or_args' && !(-t STDIN)) {
                    unless (defined($r->{args}{$an})) {
                        $r->{args}{$an} = $do_stream ?
                            __gen_iter(\*STDIN, $as, $an) :
                            $is_ary ? [map {chomp;$_} <STDIN>] :
                                do {local $/; ~~<STDIN>};
                    }
                } elsif ($src eq 'file') {
                    unless (exists $r->{args}{$an}) {
                        if ($as->{req}) {
                            die [400,
                                 "Please specify filename for argument '$an'"];
                        } else {
                            next;
                        }
                    }
                    die [400, "Please specify filename for argument '$an'"]
                        unless defined $r->{args}{$an};
                    #log_trace("Getting argument '$an' value from ".
                    #                "file ...");
                    my $fh;
                    my $fname = $r->{args}{$an};
                    unless (open $fh, "<", $fname) {
                        die [500, "Can't open file '$fname' for argument '$an'".
                                 ": $!"];
                    }
                    $r->{args}{$an} = $do_stream ?
                        __gen_iter($fh, $as, $an) :
                            $is_ary ? [<$fh>] :
                                do { local $/; ~~<$fh> };
                    close $fh;
                    $r->{args}{"-cmdline_src_$an"} = 'file';
                    $r->{args}{"-cmdline_srcfilenames_$an"} = [$fname];
                }
            }

            # encode to base64 if binary and we want to cross network (because
            # it's usually JSON)
            if ($self->riap_version == 1.2 && $is_network &&
                    defined($r->{args}{$an}) && $args_p->{$an}{schema} &&
                        $args_p->{$an}{schema}[0] eq 'buf' &&
                            !$r->{args}{"$an:base64"}) {
                require MIME::Base64;
                $r->{args}{"$an:base64"} =
                    MIME::Base64::encode_base64($r->{args}{$an}, "");
                delete $r->{args}{$an};
            }
        } # for arg
    }
    #log_trace("args after cmdline_src is processed: %s", $r->{args});
}

# determine filehandle to output to (normally STDOUT, but we can also send to a
# pager, or a temporary file when sending to viewer (the difference between
# pager and viewer: when we page we use pipe, when we view we write to temporary
# file then open the viewer. viewer settings override pager settings.
sub select_output_handle {
    my ($self, $r) = @_;

    my $resmeta = $r->{res}[3] // {};

    my $handle;
  SELECT_HANDLE:
    {
        # view result using external program
        if ($ENV{VIEW_RESULT} // $resmeta->{"cmdline.view_result"}) {
            my $viewer = $resmeta->{"cmdline.viewer"} // $ENV{VIEWER} //
                $ENV{BROWSER};
            last if defined $viewer && !$viewer; # ENV{VIEWER} can be set 0/'' to disable viewing result using external program
            die [500, "No VIEWER program set"] unless defined $viewer;
            $r->{viewer} = $viewer;
            require File::Temp;
            my $filename;
            ($handle, $filename) = File::Temp::tempfile();
            $r->{viewer_temp_path} = $filename;
        }

        if ($ENV{PAGE_RESULT} // $resmeta->{"cmdline.page_result"}) {
            require File::Which;
            my $pager = $resmeta->{"cmdline.pager"} //
                $ENV{PAGER};
            unless (defined $pager) {
                $pager = "less -FRSX" if File::Which::which("less");
            }
            unless (defined $pager) {
                $pager = "more" if File::Which::which("more");
            }
            unless (defined $pager) {
                die [500, "Can't determine PAGER"];
            }
            last unless $pager; # ENV{PAGER} can be set 0/'' to disable paging
            #log_trace("Paging output using %s", $pager);
            ## no critic (InputOutput::RequireBriefOpen)
            open $handle, "| $pager";
        }
        $handle //= \*STDOUT;
    }
    $r->{output_handle} = $handle;
}

sub save_output {
    my ($self, $r, $dir) = @_;
    $dir //= $ENV{PERINCI_CMDLINE_OUTPUT_DIR};

    unless (-d $dir) {
        warn "Can't save output to $dir: doesn't exist or not a directory,skipped saving program output";
        return;
    }

    my $time = do {
        if (eval { require Time::HiRes; 1 }) {
            Time::HiRes::time();
        } else {
            time();
        }
    };

    my $fmttime = do {
        my @time = gmtime($time);
        sprintf(
            "%04d-%02d-%02dT%02d%02d%02d.%09dZ",
            $time[5]+1900,
            $time[4]+1,
            $time[3],
            $time[2],
            $time[1],
            $time[0],
            ($time - int($time))*1_000_000_000,
        );
    };

    my ($fpath_out, $fpath_meta);
    my ($fh_out, $fh_meta);
    {
        require Fcntl;
        my $counter = -1;
        while (1) {
            if ($counter++ >= 10_000) {
                warn "Can't create file to save program output, skipped saving program output";
                return;
            }
            my $fpath_out  = "$dir/" . ($counter ? "$fmttime.out.$counter"  : "$fmttime.out");
            my $fpath_meta = "$dir/" . ($counter ? "$fmttime.meta.$counter" : "$fmttime.meta");
            if ((-e $fpath_out) || (-e $fpath_meta)) {
                next;
            }
            unless (sysopen $fh_out , $fpath_out , Fcntl::O_WRONLY() | Fcntl::O_CREAT() | Fcntl::O_EXCL()) {
                warn "Can't create file '$fpath_out' to save program output: $!, skipped saving program output";
                return;
            }
            unless (sysopen $fh_meta, $fpath_meta, Fcntl::O_WRONLY() | Fcntl::O_CREAT() | Fcntl::O_EXCL()) {
                warn "Can't create file '$fpath_meta' to save program output meta information: $!, skipped saving program output";
                unlink $fpath_out;
                return;
            }
            last;
        }
    }

    require JSON::MaybeXS;
    state $json = JSON::MaybeXS->new->allow_nonref;

    my $out = $self->cleanser->clone_and_clean($r->{res});
    my $meta = {
        time        => $time,
        pid         => $$,
        argv        => $r->{orig_argv},
        read_env    => $r->{read_env},
        read_config => $r->{read_config},
        read_config_files => $r->{read_config_files},
    };
    log_trace "Saving program output to %s ...", $fpath_out;
    print $fh_out $json->encode($out);
    log_trace "Saving program output's meta information to %s ...", $fpath_meta;
    print $fh_meta $json->encode($meta);
}

sub display_result {
    require Data::Sah::Util::Type;

    my ($self, $r) = @_;

    my $meta = $r->{meta};
    my $res = $r->{res};
    my $fres = $r->{fres};
    my $resmeta = $res->[3] // {};

    my $handle = $r->{output_handle};

    my $sch = $meta->{result}{schema};
    my $type = Data::Sah::Util::Type::get_type($sch) // '';

    if ($resmeta->{stream} // $meta->{result}{stream}) {
        my $x = $res->[2];
        if (ref($x) eq 'CODE') {
            if (Data::Sah::Util::Type::is_simple($sch)) {
                while (defined(my $l = $x->())) {
                    print $l;
                    print "\n" unless $type eq 'buf';
                }
            } else {
                require JSON::MaybeXS;
                state $json = JSON::MaybeXS->new->allow_nonref;
                if ($self->use_cleanser) {
                    while (defined(my $rec = $x->())) {
                        print $json->encode(
                            $self->cleanser->clone_and_clean($rec)), "\n";
                    }
                } else {
                    while (defined(my $rec = $x->())) {
                        print $json->encode($rec), "\n";
                    }
                }
            }
        } else {
            die "Result is a stream but no coderef provided";
        }
    } else {
        print $handle $fres;
        if ($r->{viewer}) {
            require ShellQuote::Any::Tiny;
            my $cmd = $r->{viewer} ." ". ShellQuote::Any::Tiny::shell_quote($r->{viewer_temp_path});
            system $cmd;
        }
    }
}

sub run {
    my ($self) = @_;
    log_trace("[pericmd] -> run(), \@ARGV=%s", \@ARGV);

    my $co = $self->common_opts;

    my $r = {
        orig_argv   => [@ARGV],
        common_opts => $co,
    };

    # dump is special case, we delegate to do_dump()
    if ($ENV{PERINCI_CMDLINE_DUMP}) {
        $r->{res} = $self->do_dump($r);
        goto FORMAT;
    }

    # completion is special case, we delegate to do_completion()
    if ($self->_detect_completion($r)) {
        $r->{res} = $self->do_completion($r);
        goto FORMAT;
    }

    if ($self->read_config) {
        # note that we will be reading config file
        $r->{read_config} = 1;
    }

    if ($self->read_env) {
        # note that we will be reading env for default options
        $r->{read_env} = 1;
    }

    eval {
        log_trace("[pericmd] Running hook_before_run ...");
        $self->hook_before_run($r);

        log_trace("[pericmd] Running hook_before_parse_argv ...");
        $self->hook_before_parse_argv($r);

        my $parse_res = $self->parse_argv($r);
        if ($parse_res->[0] == 501) {
            # we'll need to send ARGV to the server, because it's impossible to
            # get args from ARGV (e.g. there's a cmdline_alias with CODE, which
            # has been transformed into string when crossing network boundary)
            $r->{send_argv} = 1;
        } elsif ($parse_res->[0] != 200) {
            die $parse_res;
        }
        $r->{parse_argv_res} = $parse_res;
        $r->{args} = $parse_res->[2] // {};

        # set defaults
        $r->{action} //= 'call';

        # init logging
        if ($self->log) {
            require Log::ger::App;
            my $default_level = do {
                my $dry_run = $r->{dry_run} // $self->default_dry_run;
                $dry_run ? 'info' : 'warn';
            };
            Log::ger::App->import(
                level => $r->{log_level} // $self->log_level // $default_level,
                name  => $self->program_name,
            );
        }

        log_trace("[pericmd] Running hook_after_parse_argv ...");
        $self->hook_after_parse_argv($r);

        $self->parse_cmdline_src($r);

        #log_trace("TMP: parse_res: %s", $parse_res);

        my $missing = $parse_res->[3]{"func.missing_args"};
        die [400, "Missing required argument(s): ".join(", ", @$missing)]
            if $missing && @$missing;

        my $scd = $r->{subcommand_data};
        if ($scd->{pass_cmdline_object} // $self->pass_cmdline_object) {
            $r->{args}{-cmdline} = $self;
            $r->{args}{-cmdline_r} = $r;
        }

        log_trace("[pericmd] Running hook_before_action ...");
        $self->hook_before_action($r);

        my $meth = "action_$r->{action}";
        die [500, "Unknown action $r->{action}"] unless $self->can($meth);
        log_trace("[pericmd] Running %s() ...", $meth);
        $r->{res} = $self->$meth($r);
        #log_trace("[pericmd] res=%s", $r->{res}); #1

        log_trace("[pericmd] Running hook_after_action ...");
        $self->hook_after_action($r);
    };
    my $err = $@;
    if ($err || !$r->{res}) {
        if ($err) {
            $err = [500, "Died: $err"] unless ref($err) eq 'ARRAY';
            if (%Devel::Confess::) {
                no warnings 'once';
                require Scalar::Util;
                my $id = Scalar::Util::refaddr($err);
                my $stack_trace = $Devel::Confess::MESSAGES{$id};
                $err->[1] .= "\n$stack_trace" if $stack_trace;
            }
            $err->[1] =~ s/\n+$//;
            $r->{res} = $err;
        } else {
            $r->{res} = [500, "Bug: no response produced"];
        }
    } elsif (ref($r->{res}) ne 'ARRAY') {
        log_trace("[pericmd] res=%s", $r->{res}); #2
        $r->{res} = [500, "Bug in program: result not an array"];
    }

    if (!$r->{res}[0] || $r->{res}[0] < 200 || $r->{res}[0] > 555) {
        $r->{res}[3]{'x.orig_status'} = $r->{res}[0];
        $r->{res}[0] = 555;
    }

    $r->{format} //= $r->{res}[3]{'cmdline.default_format'};
    $r->{format} //= $r->{meta}{'cmdline.default_format'};
    my $restore_orig_result;
    my $orig_result;
    if (exists $r->{res}[3]{'cmdline.result'}) {
        # temporarily change the result for formatting
        $restore_orig_result = 1;
        $orig_result = $r->{res}[2];
        $r->{res}[2] = $r->{res}[3]{'cmdline.result'};
    }
  FORMAT:
    my $is_success = $r->{res}[0] =~ /\A2/ || $r->{res}[0] == 304;

    if (defined $ENV{PERINCI_CMDLINE_OUTPUT_DIR}) {
        $self->save_output($r);
    }

    if ($is_success &&
            ($self->skip_format ||
             $r->{meta}{'cmdline.skip_format'} ||
             $r->{res}[3]{'cmdline.skip_format'})) {
        $r->{fres} = $r->{res}[2] // '';
    } elsif ($is_success &&
                 ($r->{res}[3]{stream} // $r->{meta}{result}{stream})) {
        # stream will be formatted as displayed by display_result()
    }else {
        log_trace("[pericmd] Running hook_format_result ...");
        $r->{res}[3]{stream} = 0;
        $r->{fres} = $self->hook_format_result($r) // '';
    }
    $self->select_output_handle($r);
    log_trace("[pericmd] Running hook_display_result ...");
    $self->hook_display_result($r);
    log_trace("[pericmd] Running hook_after_run ...");
    $self->hook_after_run($r);

    if ($restore_orig_result) {
        $r->{res}[2] = $orig_result;
    }

    my $exitcode;
    if ($r->{res}[3] && defined($r->{res}[3]{'cmdline.exit_code'})) {
        $exitcode = $r->{res}[3]{'cmdline.exit_code'};
    } else {
        $exitcode = $self->status2exitcode($r->{res}[0]);
    }
    if ($self->exit) {
        log_trace("[pericmd] exit(%s)", $exitcode);
        exit $exitcode;
    } else {
        # so this can be tested
        log_trace("[pericmd] <- run(), exitcode=%s", $exitcode);
        $r->{res}[3]{'x.perinci.cmdline.base.exit_code'} = $exitcode;
        return $r->{res};
    }
}

1;
# ABSTRACT: Rinci/Riap Plack/CGI application framework

=for Pod::Coverage ^(.+)$
