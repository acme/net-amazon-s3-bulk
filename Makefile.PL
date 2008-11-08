#!perl
use strict;
use warnings;
use ExtUtils::MakeMaker;
WriteMakefile(
    NAME         => 'Net::Amazon::S3::Bulk',
    VERSION_FROM => 'lib/Net/Amazon/S3/Bulk.pm',
    AUTHOR       => 'Leon Brocard <acme@astray.com>',
    ABSTRACT =>
        'Use the Amazon S3 - Simple Storage Service for bulk uploading and downloading',
    LICENSE   => 'perl',
    PREREQ_PM => {
        'Net::Amazon::S3' => '0.46',
        'Mojo'            => '0.8008',
    }
);

