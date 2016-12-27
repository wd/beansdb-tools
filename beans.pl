#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use File::Basename;

use QLZ qw/compress decompress/;
$| = 1;

my $PADDING = 256;
my $COMPRESS_FLAG = 0x00010000;
my $MAX_INT32 = 0xffffffff;

my $o = {
    'verbose' => 0,
    'help' => sub {  &usage && exit },
};

sub usage {
    my $cmd = basename $0;

    print <<__USAGE__;
$cmd - beansdb maintance script

Usage:  $cmd [options..]
Options:
        --help, --usage                     Print This mesage
        --verbose, -v                       Print more debug info (default: $o->{verbose} )

        --data_directory, -d                Beansdb data file directory to read

        --merge, -m                         Do merge
        --expire_days,-e                    Keys expire before these days
        --size_limit,-s                     Keys expire only size greater then this limit, like 10M
        --expire_range,-r                   Keys expire range in forms like 10M:30,50M:10, which means delete keys set at
                                            30 days ago and size between 10M ~ 50M, 10 days when size greater 50M

        --print_all_keys, -p                Print all keys

        --build_hint, -b                    Build hint file

        --validate_hint, -c                 Check keys from hint and data
        --validate_hint_tmp, -t             Check keys from hint and tmp hint file

        --test                              Run test case

Examples:
    $cmd -v -d /opt/beansdb/var/data_beans1/0/0 -m -e 30 -s 1M //delete keys set 30 days ago and size greater then 1M

__USAGE__
}

sub log {
    my $msg = shift;
    print localtime() . " $msg\n" if $o->{verbose};
}

sub get_options {
    GetOptions($o,
               'verbose|v',
               'help|usage',
               'print_all_keys|p',
               'merge|m',
               'data_directory|d=s',
               'expire_days|e=i',
               'build_hint|b',
               'validate_hint|c',
               'validate_hint_tmp|t',
               'size_limit|s=s',
               'expire_range|r=s',
               'test',
    );
}

sub main {
    &get_options;

    if ( $o->{test} ) {
        &test;
        exit;
    }

    &usage && die "\nneed beansdb data directory to go on" if ( ! $o->{data_directory} );

    my $input_dir = $o->{data_directory};
    my @files = sort glob "$input_dir/*.data";

    if ( $o->{merge} || $o->{print_all_keys} ) {
        my $ranges;
        if ( $o->{expire_range} ) {
            $ranges = &check_expire_range;
            &usage && die "\nexpire range error" if ( !$ranges );
        }

        &log("scan all keys for $input_dir");
        my ( $total, $idx, $del, $files_need_process) = &get_index_data(\@files, $ranges);

        if ( $o->{print_all_keys} ) {
            for my $k ( keys %$idx ) {
                print sprintf("%s	%s	%s\n", $k, $idx->{$k}->{ver}, $idx->{$k}->{datapos});
            }
            exit;
        }

        if ( $o->{merge} ) {
            &log("total " . ( keys %$del ) . "/$total can delete");
            if ( %$files_need_process ) {
                &log("files need process " . join(' ', keys %$files_need_process));
                &do_merge( $input_dir, $del, keys %$files_need_process );
            } else {
                &log("no files need process");
            }
            exit;
        }
    }

    if ( $o->{build_hint} ) {
        &log("build hint file for $input_dir");
        for my $file ( @files ) {
            my $hint_file = &get_hint_file_name($file);
            my $tmp_hint_file = $hint_file . ".tmp";

            open my $fo, ">", $tmp_hint_file or die "can't open file $tmp_hint_file : $!";
            binmode $fo;

            my $idx = &scan_data_file($file);
            &log("build hint file data ...");
            my $hint_data = '';
            for my $key ( keys %$idx ) {
                my $rec = $idx->{$key};
                $rec->{key} = $key;
                $hint_data .= &build_hint_record($rec);
            }
            my $comp_data = compress($hint_data);
            print $fo $comp_data;
            close($fo);
            &log("done");
        }
        exit;
    }

    if ( $o->{validate_hint} ) {
        &log("validating hint file for $input_dir");
        for my $file ( @files ) {
            &log("process $file");
            my $idx = &scan_data_file($file);
            my $hint_file = &get_hint_file_name($file);
            my $hint_idx = &scan_hint_file($hint_file);
            my $diff = 0;
            for my $key ( keys %$idx ) {
                if ( defined $hint_idx->{$key} ) {
                    for my $prop ( qw/datapos ver/ ) {
                        my $a = $idx->{$key}->{$prop};
                        my $b = $hint_idx->{$key}->{$prop};
                        if ( $a ne $b ) {
                            print "$a ne $b in hint\n";
                            $diff++;
                        }
                    }
                } else {
                    $diff ++;
                }
            }
            &log("total $diff diff");
        }
        exit;
    }

    if ( $o->{validate_hint_tmp} ) {
        &log("validating hint file for $input_dir");
        for my $file ( @files ) {
            &log("process $file");
            my $hint_file = &get_hint_file_name($file);
            my $tmp_hint_file = $hint_file . ".tmp";
            next if ( !-f $hint_file );
            my $hint_idx = &scan_hint_file($hint_file);
            my $tmp_hint_idx = &scan_hint_file($tmp_hint_file);

            my $diff = 0;
            for my $key ( keys %$hint_idx ) {
                if ( defined $tmp_hint_idx->{$key} ) {
                    for my $prop ( qw/datapos ver hash ksz/ ) {
                        my $a = $hint_idx->{$key}->{$prop};
                        my $b = $tmp_hint_idx->{$key}->{$prop};
                        if ( $a ne $b ) {
                            print "$key $prop:$a ne $b(tmp) in hint\n";
                            $diff++;
                        }
                    }
                } else {
                    $diff ++;
                }
            }
            &log("total $diff diff");
        }
        exit;
    }
}

sub do_merge {
    my ( $input_dir, $del_keys, @files ) = @_;
    for my $tag ( @files ) {
        my $file = "$input_dir/$tag";
        my $hint_file = &get_hint_file_name($file);

        &log("doing merge for $file");

        my $tmp_file = $file . '.tmp';
        my $tmp_hint_file = $hint_file . '.tmp';
        open my $fh, '<', $file or die "can't open input file $file : $!";
        binmode $fh;

        open my $fo, '>', $tmp_file or die "can't open output file $tmp_file : $!";
        binmode $fo;

        my ( $del, $total, $expire ) = ( 0, 0, 0 );
        my $hint_data = '';
        while (!eof($fh)) {
            my $data = &read_data($fh);

            next if not defined $data->{key};

            my $del_type = $del_keys->{ join('', ( $tag, $data->{key} , $data->{ver} ) ) };
            if ( not defined $del_type ) {
                &write_data($fo, $data);
                $hint_data .= &build_hint_record($data);
            } else {
                if ( $del_type == 1 ) {
                    $del++;
                }else {
                    $expire++;
                }
            }
            $total ++;
        }

        close($fh);
        close($fo);

        rename $tmp_file, $file;
        rename $tmp_hint_file, $hint_file;

        &log("$total keys processed, $del deleted, $expire expired");
    }
}

sub get_index_data {
    my ($files, $ranges) = @_;

    my $files_idx = {};
    for my $file ( @$files ) {
        my $tag = (split('/', $file))[-1];
        &log("process $file");

        my $hint_file = &get_hint_file_name($file);
        if ( defined $o->{expire_days} || defined $o->{size_limit} || defined $o->{expire_range} ) {
            &log("try to delete expire keys, have to scan data file" );
            $files_idx->{$tag} = &scan_data_file($file);
        } elsif ( -f $hint_file ) {
            $files_idx->{$tag} = &scan_hint_file($hint_file);
        } else {
            &log("no hint file, scan data file");
            $files_idx->{$tag} = &scan_data_file($file);
        }
    }

    my $total = 0;
    my ( %idx, %del, %files_need_process );
    &log("merge each files idx");
    for my $tag ( sort keys %$files_idx ) {
        for my $key ( keys %{$files_idx->{$tag}} ) {
            $total ++;
            my $data = $files_idx->{$tag}->{$key};

            if ( defined $idx{$key} ) { # put old version in hash
                $files_need_process{ $idx{$key}->{tag} } = 1;
                $del{ join( '', ( $idx{$key}->{tag}, $key, $idx{$key}->{ver} ) ) } = 1;
            } elsif ( $data->{ver} < 0 ) { # put deleted version in hash
                $files_need_process{ $tag } = 1;
                $del{ join( '', ( $tag, $key, $data->{ver} ) ) } = 1;
            } elsif ( defined $data->{tstamp} && &if_expire_key($ranges, $data->{tstamp}, $data->{vsz}) ) {
                $files_need_process{ $tag } = 1;
                $del{ join( '', ( $tag, $key, $data->{ver} ) ) } = 2;
            }

            $idx{$key} = {
                datapos => $data->{datapos},
                tag => $tag,
                ver => $data->{ver},
            };
            &log("processed $total") if $total % 1000 == 0;
        }
    }
    &log("total processed $total");
    return $total, \%idx, \%del, \%files_need_process;
}

sub if_expire_key {
    my ( $ranges, $key_tstamp, $value_size ) = @_;

    return 0 if not $ranges;

    my $i = 0;
    for my $range ( @$ranges ) {
        $i++;
        my $size = &convert_size($range->{size});
        my $expire_time = time - $range->{day} * 24 * 3600;

        if ( $value_size >= $size ) {
            if ($expire_time >= $key_tstamp) {
                # delete keys before expire time and size greater then limit size
                return 1;
            } else {
                return 0;
            }
        }
    }
    return 0;
}

sub convert_size {
    my $size = shift;
    my %unit_hash = ( M => 1024 * 1024, K => 1024, 0 => 1 );
    if ( $size =~ m/^([0-9]+)(M|K)?$/i ) {
        my $size_num = $1;
        my $size_unit = uc($2 || 0);
        return $size_num * $unit_hash{$size_unit};
    } else {
        return $size;
    }
}

sub check_expire_range {
    my ( $expire_time, $size_limit, $expire_range ) = ( 0, 0, '');
    $expire_time = $o->{expire_days} if defined $o->{expire_days};
    $size_limit = $o->{size_limit} if defined $o->{size_limit};
    $expire_range = $o->{expire_range};

    my @ranges;
    my $valid = 1;
    if ( $expire_time ) {
        if ( $size_limit =~ m/^[0-9]+(M|K)?$/i ) {
            @ranges = ( { size => uc($size_limit), day => $expire_time } );
        } else {
            &log("size_limit $size_limit is invalid");
            $valid = 0;
        }
    }

    for my $range ( split(",", $o->{expire_range}) ) {
        my ( $size, $day ) = split(":", $range, 2);
        if (( $size !~ m/^[0-9]+(M|K)?$/i )
            || ( $day !~ m/^[0-9]+$/ ) ) {
            &log("$range is not a valid range");
            $valid = 0;
        } else {
            push @ranges, { size => uc($size), day => $day };
        }
    }

    @ranges = sort { &convert_size($b->{size}) <=> &convert_size($a->{size}) } @ranges;

    if ( $valid ) {
        &log("ranges: " . Dumper(\@ranges) );
        return \@ranges;
    } else {
        return 0;
    }
}

sub scan_data_file {
    my ($file) = @_;
    &log("scan $file ...");

    open my $fh, '<', $file or die "can't open input file $file : $!";
    binmode $fh;

    my $i = 0;
    my %idx;
    while (!eof($fh)) {
        my $data = &read_data($fh);
        my $key = $data->{key};

        next if not defined $key;

        $i++;

        $idx{$data->{key}} = {
            datapos => $data->{datapos},
            crc => $data->{crc},
            ver => $data->{ver},
            tstamp => $data->{tstamp},
            ksz => $data->{ksz},
            vsz => $data->{vsz},
            hash => $data->{hash},
        };

        &log("processed $i") if $i % 1000 == 0;
    }
    &log("total processed $i");

    close($fh);
    return  \%idx;
}

sub read_data {
    my ( $fh ) = @_;
    my $start_pos = tell($fh);

    read($fh, my $header, 4*6);
    my ( $crc, $tstamp, $flag, $ver, $ksz, $vsz ) = unpack('I i i i I I', $header);
    my $rec = { datapos => $start_pos, crc => $crc, tstamp => $tstamp, flag => $flag, ver => $ver, ksz => $ksz, vsz => $vsz };

    #&log( sprintf("pos $start_pos, ksz: %s, vsz: %s, key: %s", $rec->{ksz}, $rec->{vsz}, '111') );

    my $ret;
    if ( $rec->{crc} ) {

        read($fh, my $key, $rec->{ksz});
        read($fh, my $value, $rec->{vsz});

        $rec->{key} = $key;
        $rec->{value} = $value;


        $rec->{data} = $header . $key . $value;


        if ( $rec->{flag} & $COMPRESS_FLAG ) {
            $rec->{orig_value} = decompress($rec->{value});
            $rec->{orig_vsz} = length($rec->{orig_value});
            $rec->{hash} = &gen_hash($rec->{orig_value});
        } else {
            $rec->{hash} = &gen_hash($rec->{value});
        }

        $ret = $rec;
    } else {
        $ret = undef;
        &log("invalid key at $start_pos");
    }

    $rec->{total_size} = 4 * 6 + $rec->{ksz} + $rec->{vsz};

    my $pad = $rec->{total_size} % $PADDING;

    if ( $pad ) {
        seek($fh, $PADDING - $pad, 1);
    }

    #$rec->{orig_value} = '';
    #$rec->{data} = '';
    #$rec->{value} = '';

    return $ret;
}

sub write_data {
    my ( $fh, $rec ) = @_;
    print $fh $rec->{data};
    my $pad = $rec->{total_size} % $PADDING;
    if ( $pad ) {
        my $more = $PADDING - $pad;
        print $fh pack("a$more", "\0");
    }
}

sub scan_hint_file {
    my $file = shift;
    &log("scan $file ...");
    open my $fh, '<', $file or die "cant' open file $file : $!";
    binmode $fh;

    my $comp_data = '';
    while (my $bytesread = read($fh , my $buffer, 1024)) {
        $comp_data .= $buffer;
    }

    my $data = decompress($comp_data);

    my $pos = 0;
    my %idx;
    my $i = 0;
    while ( $pos < length($data) - 1 ) {
        my $rec;
        $i++;
        ( $rec, $pos) = &read_hint_record($data, $pos);

        $idx{$rec->{key}} = {
            datapos => $rec->{datapos},
            hash => $rec->{hash},
            ver => $rec->{ver},
            ksz => $rec->{ksz},
        };
        &log("processed $i") if $i % 1000 == 0;
    }
    &log("total processed $i");

    return \%idx;
}

sub read_hint_record {
    my ( $data, $pos ) = @_;

    my $size = (8 + 24 + 32 + 16)/8; #10 byte
    my $header = substr($data, $pos, $size);
    $pos += $size;

    my ( $ksz, $datapos, $ver, $hash ) = unpack("B8 B24 i B16", $header);

    $ksz = unpack("I", pack("B32", $ksz));
    $datapos = unpack("I", pack("B32", $datapos));
    #$datapos = ($datapos << 8) | (bucket & 0xff);
    $datapos = $datapos << 8;
    $hash = unpack("I", pack("B32", $hash));

    my $rec = { ksz => $ksz, datapos => $datapos, ver => $ver, hash => $hash };

    my $key = substr($data, $pos, $rec->{ksz});
    $pos += $rec->{ksz};
    my $padding = substr($data, $pos, 1);
    $pos ++;

    $rec->{key} = $key;

    return $rec, $pos;
}

sub build_hint_record {
    my $rec = shift;

    my $ksz = unpack("B8", pack("I", $rec->{ksz}));
    my $datapos = unpack("B24", pack("I", $rec->{datapos} >> 8));
    my $ver = unpack("B32", pack("i", $rec->{ver}));
    my $hash = unpack("B16", pack("I", $rec->{hash}));

    my $key = $rec->{key};

    my $data = pack("B*", $ksz . $datapos . $ver . $hash) . $key . pack("B", 0x00);
    my $comp_data = compress($data);
    return $data;
}

sub get_hint_file_name {
    my $file = shift;
    my $hint_file = substr($file, 0, -4 ) . 'hint.qlz';
    return $hint_file;
}

sub gen_hash {
    my $data = shift;;
    my $len = length($data);
    my $hash = $len * 97;
    if ( $len <= 1024 ) {
        $hash += fnv1a($data);
    } else {
        $hash += fnv1a( substr($data, 0, 512) );
        $hash = ($hash * 97 ) & $MAX_INT32;
        $hash += fnv1a( substr($data, -512) );
    }
    return $hash;
}

sub fnv1a {
    my $s = shift;

    my $prime = 0x01000193;
    my $h = 0x811c9dc5;

    for my $c ( split(//,$s) ) {
        #$h ^= ord($c);
        $h = ( $h ^ unpack('c', $c) ) & $MAX_INT32;
        $h = ( $h * $prime ) & $MAX_INT32;
    }
    return $h;
}

sub test {
    use Test::Base -Base;
    use Test::More;

    #plan tests => 2 * blocks;

    for my $b ( blocks ) {
        $o = $b->{options}->[0];
        my $ranges = check_expire_range;
        my @exps = @{$b->{expected}->[0]};
        my @keys = @{$b->{'keys'}->[0]};

        note('tests for ' . $b->{name}->[0]);

        for ( my $i = 0; $i< scalar @keys; $i++ ) {
            my ( $key_tstamp, $value_size ) = @{$keys[$i]};
            my $ret = if_expire_key($ranges, $key_tstamp, $value_size);
            my $exp = $exps[$i];
            chomp($exp);
            is($ret, $exp);
        }
    }

    done_testing();
}

&main;

__END__
=== 小于最小区间，没有限制
--- options eval
{ 'expire_range' => '10k:30,10m:11' }
--- keys eval
[ [ time - 10 * 24 * 3600, 9 * 1024 ], [ time - 40 * 24 * 3600, 6 * 1024 ] ]
--- expected eval
[ 0, 0 ]

=== 大于最小区间，小于第二个区间，受最小区间限制
--- options eval
{ 'expire_range' => '10k:30,10m:11' }
--- keys eval
[ [ time - 10 * 24 * 3600, 11 * 1024 ], [ time - 40 * 24 * 3600, 9 * 1024 * 1024 ] ]
--- expected eval
[ 0, 1 ]

=== 大于最大区间，受最大区间限制
--- options eval
{ 'expire_range' => '10k:30,10m:11' }
--- keys eval
[ [ time - 10 * 24 * 3600, 11 * 1024 * 1024 ], [ time - 40 * 24 * 3600, 12 * 1024 * 1024 ] ]
--- expected eval
[ 0, 1 ]

=== 最小区间由 expire_days 指定
--- options eval
{ 'expire_range' => '10k:30,10m:11', 'expire_days' => 9 }
--- keys eval
[ [ time - 10 * 24 * 3600, 9 * 1024 ], [ time - 8 * 24 * 3600, 6 * 1024 ] ]
--- expected eval
[ 1, 0 ]
