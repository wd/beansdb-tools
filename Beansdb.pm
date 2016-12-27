#!/bin/env perl
package Beansdb;

use strict;
use warnings;
use Cache::Memcached;
use Data::Dumper;
use Carp;

#
# my $servers = { 'localhost:7900' => [1, 2, 3, 4, 5 ], 'localhost:7901' => [1, 2, 3, 4, 5 ] }
# 上面 [1-5] 定义每个 server 在哪个桶里面
# my $beans = Beansdb->new( $servers, 5 )
# 这里定义总共有几个桶
#
#

my $DEBUG = $ENV{'DEBUG'} || 0;

sub new {
    my ( $class, $servers, $buckets_count, $N, $W, $R ) = @_;

    die "servers is needed!" if not $servers;

    my $self = {};
    $self->{hash_space} = 1<<32;
    $self->{buckets_count} = defined $buckets_count ? $buckets_count : 16;
    $self->{cached} = 1;
    $self->{servers} = {};
    $self->{server_buckets} = {};
    $self->{bucket_size} = int($self->{hash_space} / $self->{buckets_count});
    $self->{buckets} = {}; # 用来存放每个桶里面有哪些 server
    $self->{N} = $N || 3;
    $self->{W} = $W || 1;
    $self->{R} = $R || 1;

    while ( my  ($server, $bs) = each ( %$servers ) ) {
        print "connect $server \n" if $DEBUG;
        my $mem = MCStore->new($server);
        carp "Server $server connect failed!" if ( !$mem );
        $self->{servers}->{$server} = $mem;
        $self->{server_buckets}->{$server} = $bs;
        for my $b ( @$bs ) {
            if ( not defined $self->{buckets}->{$b} ) {
                $self->{buckets}->{$b} = [];
            }
            push @{ $self->{buckets}->{$b} }, $mem;
        }
    }

    for ( my $i =0; $i < $self->{buckets_count} - 1; $i++ ) {
        my @mems = sort {
            BeansdbUtils::fnv1a($a->{server} ) <=> BeansdbUtils::fnv1a($b->{server})
        } @{ $self->{buckets}->{$i} };

        $self->{buckets}->{$i} = \@mems;
    }

    bless $self, $class;
    return $self;
}

sub print_buckets {
    my $self = shift;

    while (my ($bs, $servers ) = each %{$self->{buckets}} ) {
        print $bs . ": " . join(',', map { $_->{server} } @$servers) . "\n";
    }

    for my $server ( keys %{$self->{server_buckets} } ) {
        print "$server " . scalar @{ $self->{server_buckets}->{$server} } . "\n";
    }
}

sub _get_servers {
    my ( $self, $key ) = @_;
    my $hash = BeansdbUtils::fnv1a($key);
    my $b = int($hash / $self->{bucket_size});
    return $self->{buckets}->{$b};
}

sub get {
    my ( $self, $key ) = @_;
    my $servers = $self->_get_servers($key);
    my $count = 0;
    for my $server ( @$servers ) {
        my $r = $server->get($key);
        if ( defined $r ) {
            # 如果有结果返回，就把他前面的 server set 一下，因为前面肯定没取到结果, 自恢复
            for ( my $i =0; $i < $count; $i++) {
                print "self heal server $i\n" if $DEBUG;
                my $s = $servers->[$i];
                $s->set($key, $r);
            }
            return $r;
        }
        print "Server " . $server->{server} . " result not found\n" if $DEBUG;
        $count ++;
    }
}

sub set {
    my ( $self, $key, $value ) = @_;
    my $servers = $self->_get_servers($key);

    my $count = 0;
    for my $server ( @$servers ) {
        my $rt = $server->set($key, $value);
        $count ++ if ( $rt );
    }

    if ( not $count >= $self->{W} ) {
        print "Server set succ count = $count less then except " . $self->{W} . "\n" if $DEBUG;
        my $r = $self->get($key);
        if ( (not defined $r ) || $r ne $value ) {
            #carp("write error");
            return 0;
        }
    }
    return 1;
}

package BeansdbUtils;

sub fnv1a {
    my $s = shift;

    my $prime = 0x01000193;
    my $h = 0x811c9dc5;

    for my $c ( split(//,$s) ) {
        $h ^= ord($c);
        $h = ( $h * $prime ) & 0xffffffff;
    }
    return $h;
}

package MCStore;

sub new {
    my ( $class, $server) = @_;
    my $self = {
        'server' => $server,
        'mc' => Cache::Memcached->new( { 'servers' => [ $server ], 'debug' => $DEBUG } ),
    };
    bless $self, $class;
    return $self;
}

sub get {
    my ( $self, $key )  = @_;
    print "Get [$key] from $self->{server}\n" if $DEBUG;
    return $self->{mc}->get($key);
}

sub set {
    my ( $self, $key, $value ) = @_;
    print "set [$key] to $self->{server}\n" if $DEBUG;
    return $self->{mc}->set($key, $value);
}

1;
