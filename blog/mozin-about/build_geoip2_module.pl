#!/usr/bin/env perl
#
#   build_geoip2_module -- Quick script to convert a given JSON document to a
#                          GeoIP2 database
#
#   author: Derek Ditch <dcode@elastic.co>
#
#   Copyright (c) 2021 Elastic NV
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

use strict;
use warnings;
use MaxMind::DB::Writer::Tree;
use Net::Works::Network;
use File::Basename;

use JSON;
use Data::Dumper;

# These are hard-coded filenames. You could make these CLI arguments
my $input_filename  = "data/mozi-ipinfo.json";
my $output_filename = 'ingest-geoip/mozi-enrichment-%s.mmdb';
my $bundle_name = 'mozi-ipinfo-geoip2.zip';

my %types = (
    # These are the authoritative mappings for MaxMind GeoIP2 City database
    names                  => 'map',
    city                   => 'map',
    continent              => 'map',
    registered_country     => 'map',
    represented_country    => 'map',
    country                => 'map',
    location               => 'map',
    postal                 => 'map',
    traits                 => 'map',

    geoname_id             => 'uint32',

    type                   => 'utf8_string',
    en                     => 'utf8_string',
    de                     => 'utf8_string',
    es                     => 'utf8_string',
    fr                     => 'utf8_string',
    ja                     => 'utf8_string',
    'pt-BR'                => 'utf8_string',
    ru                     => 'utf8_string',
    'zh-CN'                => 'utf8_string',

    locales                => [ 'array', 'utf8_string' ],
    code                   => 'utf8_string',
    geoname_id             => 'uint32',
    ip_address             => 'utf8_string',
    subdivisions           => [ 'array' , 'map' ],
    iso_code               => 'utf8_string',
    environments           => [ 'array', 'utf8_string' ],
    expires                => 'uint32',
    name                   => 'utf8_string',
    time_zone              => 'utf8_string',
    accuracy_radius        => 'uint32',
    latitude               => 'float',
    longitude              => 'float',
    metro_code             => 'uint32',
    time_zone              => 'utf8_string',
    is_in_european_union   => 'utf8_string',
    is_satellite_provider   => 'utf8_string',
    is_anonymous_proxy     => 'utf8_string',

    # These are the authoritative mappings for MaxMind GeoIP2 ASN database
    autonomous_system_number => 'uint32',
    autonomous_system_organization => 'utf8_string',

    # This is the authoritative mapping for MaxMind GeoIP2 domain database
    domain => 'utf8_string',
);

# We actually generate three databases that are exactly the same, with different metadata.
# This is because Elasticsearch ingest processor determines database type by filename,
# but the MaxMind Java bindings determine by metadata.
foreach ( "City", "Country", "ASN" ) {
    my $count = 0;
    my $tree = MaxMind::DB::Writer::Tree->new(
        ip_version               => 4,
        record_size              => 24,
        # database_type has to match one of values here: https://github.com/maxmind/GeoIP2-java/blob/main/src/main/java/com/maxmind/geoip2/DatabaseReader.java#L103
        database_type            => sprintf('GeoLite2-%s', $_),
        languages                => ['en'],
        description              => { en => 'ipinfo GeoIP database' },
        map_key_type_callback    => sub { $types{ $_[0] } },
        remove_reserved_networks => 0,
    );

    # Read in JSON data from ipinfo
    open( FH, '<', $input_filename ) or die $!;
    read FH, my $json_data, -s FH;
    close(FH);

    my $data  = decode_json($json_data);

    # Build each GeoIP record
    while ( ( my $address, my $entry ) = each(%$data) ) {
        my $network =
        Net::Works::Network->new_from_string( string => "$address/32" );

        my $asn = sprintf("%d", $1) if( $entry->{org} =~ /AS(\d+)/ );
        my $as_org = sprintf("%s", $1) if( $entry->{org} =~ /AS\d+ (.*)$/);
        my $rec = {
            autonomous_system_organization    => $as_org,
            autonomous_system_number => $asn,
            domain         => $entry->{hostname}     // "",
            city => {
                names => {
                    en => $entry->{city}         // ""
                }
            },
            subdivisions  => [{
                names => {
                    en => $entry->{region}       // ""
                }
            }],
            country => {
                iso_code => $entry->{country} // "",
                names => {
                    en => $entry->{country_name} // ""
                }
            },
            location         => {
                longitude => ( split( ',', $entry->{loc} ) )[0] // "",
                latitude  => ( split( ',', $entry->{loc} ) )[1] // "",
                time_zone => $entry->{timezone} // "",
            },
            postal => {
                code => $entry->{postal} // "",
            },
        };

        $tree->insert_network( $network, $rec );
        $count += 1;
    }

    # Ensure output dir exits
    my $parent_dir = dirname($output_filename);
    mkdir($parent_dir)
    or $!{EEXIST}    # Don't die if $parent_dir exists.
    or die("Can't create directory \"$parent_dir\": $!\n");

    open my $fh, '>:raw', sprintf($output_filename, $_);
    $tree->write_tree($fh);
    close $fh;

    print "Wrote $count records to " . sprintf($output_filename, $_) . "\n";

}


# Create a Zip file elastic cloud module
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
my $zip = Archive::Zip->new();

# Add a directory
my $dir_member = $zip->addDirectory( 'ingest-geoip/' );

# Add a database from disk
foreach ( "City", "Country", "ASN" ) {
    my $file_member = $zip->addFile( sprintf($output_filename, $_));
}

# Save the Zip file
unless ( $zip->writeToFileNamed($bundle_name) == AZ_OK ) {
    die 'write error';
}
