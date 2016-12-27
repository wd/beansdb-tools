#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use File::Basename;

my $PADDING = 256;
my $COMPRESS_FLAG = 0x00010000;
my $expire_time = time - 40 * 24 * 3600; # 30 days ago

$| = 1;

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
        --print_all_keys, -p                Print all keys
__USAGE__
}

sub log {
    my $msg = shift;
    print localtime() . " $msg\n" if $o->{verbose};
}

sub main {
    GetOptions($o,
               'verbose|v',
               'help|usage',
               'print_all_keys|p',
               'merge|m',
               'data_directory|d=s',
    );

    if ( $o->{data_directory}
        && ( $o->{merge} || $o->{print_all_keys} )) {

        my $input_dir = $o->{data_directory};
        my @files = sort glob "$input_dir/*.data";

        &log("scan all keys for $input_dir");
        my ( $total, $idx, $del, $files_need_process) = &get_index_data(\@files);

        if ( $o->{print_all_keys} ) {
            for my $k ( keys %$idx ) {
                print sprintf("%s	%s	%s\n", $k, $idx->{$k}->{v}, $idx->{$k}->{s});
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
        }
    } else {
        &usage && die "\nneed beansdb data directory to go on";
    }
}

sub do_merge {
    my ( $input_dir, $del_keys, @files ) = @_;
    for my $tag ( @files ) {
        my $file = "$input_dir/$tag";
        
        &log("doing merge for $file");

        my $tmp_file = $file . '.tmp';
        open my $fh, '<', $file or die "can't open input file $file : $!";
        binmode $fh;
        
        open my $fo, '>', $tmp_file or die "can't open output file $tmp_file : $!";
        binmode $fo;
        
        my ( $del, $total, $expire ) = ( 0, 0, 0 );
        while (!eof($fh)) {
            my $data = &read_data($fh);

            next if not defined $data->{key};

            my $del_type = $del_keys->{ join('', ( $tag, $data->{key} , $data->{ver} ) ) };
            if ( not defined $del_type ) {
                &write_data($fo, $data);
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
        unlink substr($file, 0, -4 ) . 'hint.qlz';
    
        &log("$total keys processed, $del deleted, $expire expired");    
    }
}

sub get_index_data {
    my ($files) = @_;

    my %idx;
    my %del;
    my %files_need_process;
    my $total = 0;

    for my $file ( @$files ) {
        my $tag = (split('/', $file))[-1];
        &log("process $file");
        open my $fh, '<', $file or die "can't open input file $file : $!";
        binmode $fh;
        my $i = 0;
        
        while (!eof($fh)) {
            my $data = &read_data($fh);
            my $key = $data->{key};

            next if not defined $key;

            $i++;

            if ( defined $idx{$key} ) { # put old version in hash
                $files_need_process{ $idx{$key}->{f} } = 1;
                $del{ join( '', ( $idx{$key}->{f}, $key, $idx{$key}->{v} ) ) } = 1;
            } elsif ( $data->{ver} < 0 ) { # put deleted version in hash
                $files_need_process{ $tag } = 1;
                $del{ join( '', ( $tag, $key, $data->{ver} ) ) } = 1;
            } elsif ($data->{tstamp} < $expire_time) { # put expired in hash
                $files_need_process{ $tag } = 1;
                $del{ join( '', ( $tag, $key, $data->{ver} ) ) } = 2;
            }

            $idx{$data->{key}} = {
                s => $data->{s},
                f => $tag,
                v => $data->{ver},
            };
            $data = undef;
            &log("processed $i") if $i % 1000 == 0;
        }
        &log("total processed $i");
        $total += $i;
        close($fh);
    }
    return $total, \%idx, \%del, \%files_need_process;
}

sub read_data {
    my ( $fh ) = @_;
    my $start_pos = tell($fh);

    read($fh, my $header, 4*6);
    my ( $crc, $tstamp, $flag, $ver, $ksz, $vsz ) = unpack('I i i i I I', $header);
    my $rec = { s => $start_pos, crc => $crc, tstamp => $tstamp, flag => $flag, ver => $ver, ksz => $ksz, vsz => $vsz };

    #&log( sprintf("pos $start_pos, ksz: %s, vsz: %s, key: %s", $rec->{ksz}, $rec->{vsz}, '111') );

    my $ret;
    if ( $rec->{crc} ) {

        read($fh, my $key, $rec->{ksz});
        read($fh, my $value, $rec->{vsz});

        $rec->{key} = $key;
        $rec->{value} = $value;


        $rec->{data} = $header . $key . $value;


        if ( $rec->{flag} & $COMPRESS_FLAG ) {
            #&decompress($rec);
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

sub de_compress {
    my $rec = shift;
}

&main;
