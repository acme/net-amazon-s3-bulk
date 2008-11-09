package Net::Amazon::S3::Bulk;
use Moose;
use Digest::MD5 qw(md5 md5_hex);
use Digest::MD5::File qw(file_md5 file_md5_hex);
use File::stat;
use MIME::Base64 qw(encode_base64);
use Mojo;
use Mojo::Client;
use Mojo::Transaction;
use Net::Amazon::S3;
use Net::Amazon::S3::Client;
our $VERSION = '0.32';
my $AMAZON_HEADER_PREFIX = 'x-amz-';

sub upload_files {
    my ( $self, $files ) = @_;

    my $client = Mojo::Client->new;
    $client->continue_timeout(5);
    $client->keep_alive_timeout(15);
    $client->select_timeout(5);

    my @transactions;
    foreach my $file (@$files) {
        my $filename = $file->{filename};
        my $object   = $file->{object};
        my $md5_hex = $object->etag || file_md5_hex($filename);
        my $size = $object->size;
        unless ($size) {
            my $stat = stat($filename) || confess("No $filename: $!");
            $size = $stat->size;
        }

        my $md5 = pack( 'H*', $md5_hex );
        my $md5_base64 = encode_base64($md5);
        #warn "$md5_base64";
        chomp $md5_base64;

        my $conf = {
            'Content-MD5'    => $md5_base64,
            'Content-Length' => $size,
            'Content-Type'   => $object->content_type,
        };

        my $http_request = Net::Amazon::S3::Request::PutObject->new(
            s3        => $object->client->s3,
            bucket    => $object->bucket->name,
            key       => $object->key,
            value     => '',
            acl_short => 'public-read',
            headers   => $conf,
        )->http_request;
        #warn $http_request->as_string;

        my $req = $self->_request( $object, $http_request );
        #warn $req;

        my $file = Mojo::File->new;
        $file->path($filename);
        $req->content->file($file);
        #warn $req->content->file->path;
        warn $req->headers;

        my $transaction = Mojo::Transaction->new;
        $transaction->keep_alive(1);
        $transaction->req($req);

        #$req->fix_headers;

        #warn $req->headers;

        #print $req->build_body;

        push @transactions, $transaction;
    }

    $client->process_all(@transactions);
    warn $transactions[0]->res->code;
    warn $transactions[0]->res->content->file->slurp;
}

sub download_files {
    my ( $self, $files ) = @_;

    my $client = Mojo::Client->new;
    $client->continue_timeout(5);
    $client->keep_alive_timeout(15);
    $client->select_timeout(5);

    my @transactions;
    foreach my $file (@$files) {
        my $filename = $file->{filename};
        my $object   = $file->{object};

        my $http_request = Net::Amazon::S3::Request::GetObject->new(
            s3     => $object->client->s3,
            bucket => $object->bucket->name,
            key    => $object->key,
            method => 'GET',
        )->http_request;

        warn $http_request->as_string;

        my $req = $self->_request( $object, $http_request );
        warn $req;

        my $transaction = Mojo::Transaction->new;
        $transaction->keep_alive(1);
        $transaction->req($req);

        my $file = Mojo::File->new;
        $file->path($filename);
        $transaction->res->content->file($file);
        warn $transaction->res->content->file->path;

        push @transactions, $transaction;
    }
    $client->process_all(@transactions);
}

sub _request {
    my ( $self, $object, $http_request ) = @_;
    my $s3                    = $object->client->s3;
    my $aws_access_key_id     = $s3->aws_access_key_id;
    my $aws_secret_access_key = $s3->aws_secret_access_key;

    my $req = Mojo::Message::Request->new;
    $req->url->parse( $http_request->uri );
    $req->method( $http_request->method );

    #    $req->headers($mojo_headers);

    #   my $mojo_headers = Mojo::Headers->new;
    my $mojo_headers = $req->headers;
    $http_request->headers->scan(
        sub {
            my ( $key, $value ) = @_;
            if ( $key ne 'Authorization' ) {
                $mojo_headers->header( $key, $value );
            }
        }
    );
    $mojo_headers->header( 'Expect', '100-continue' );

    my $canonical_string = $self->_canonical_string(
        $req->method,    $object->bucket->name,
        $req->url->path, $req->headers
    );

    my $encoded_canonical
        = $self->_encode( $aws_secret_access_key, $canonical_string );
    $req->headers->header(
        Authorization => "AWS $aws_access_key_id:$encoded_canonical" );

    #    warn $req;
    return $req;
}

#warn $res->content->file->slurp;

# generate a canonical string for the given parameters.  expires is optional and is
# only used by query string authentication.
sub _canonical_string {
    my ( $self, $method, $bucket, $path, $headers, $expires ) = @_;
    my %interesting_headers = ();

    #    while ( my ( $key, $value ) = each %$headers ) {
    foreach my $key ( $headers->names ) {
        my $value = $headers->header($key);

        my $lk = lc $key;
        if (   $lk eq 'content-md5'
            or $lk eq 'content-type'
            or $lk eq 'date'
            or $lk =~ /^$AMAZON_HEADER_PREFIX/ )
        {
            $interesting_headers{$lk} = $self->_trim($value);
        }
    }

    # these keys get empty strings if they don't exist
    $interesting_headers{'content-type'} ||= '';
    $interesting_headers{'content-md5'}  ||= '';

    # just in case someone used this.  it's not necessary in this lib.
    $interesting_headers{'date'} = ''
        if $interesting_headers{'x-amz-date'};

    # if you're using expires for query string auth, then it trumps date
    # (and x-amz-date)
    $interesting_headers{'date'} = $expires if $expires;

    my $buf = "$method\n";
    foreach my $key ( sort keys %interesting_headers ) {
        if ( $key =~ /^$AMAZON_HEADER_PREFIX/ ) {
            $buf .= "$key:$interesting_headers{$key}\n";
        } else {
            $buf .= "$interesting_headers{$key}\n";
        }
    }

    # don't include anything after the first ? in the resource...
    $path =~ /^([^?]*)/;
    $buf .= "/$bucket$1";

    # ...unless there is an acl or torrent parameter
    if ( $path =~ /[&?]acl($|=|&)/ ) {
        $buf .= '?acl';
    } elsif ( $path =~ /[&?]torrent($|=|&)/ ) {
        $buf .= '?torrent';
    } elsif ( $path =~ /[&?]location($|=|&)/ ) {
        $buf .= '?location';
    }

    return $buf;
}

# finds the hmac-sha1 hash of the canonical string and the aws secret access key and then
# base64 encodes the result (optionally urlencoding after that).
sub _encode {
    my ( $self, $aws_secret_access_key, $str, $urlencode ) = @_;
    my $hmac = Digest::HMAC_SHA1->new($aws_secret_access_key);
    $hmac->add($str);
    my $b64 = encode_base64( $hmac->digest, '' );
    if ($urlencode) {
        return $self->_urlencode($b64);
    } else {
        return $b64;
    }
}

sub _trim {
    my ( $self, $value ) = @_;
    $value =~ s/^\s+//;
    $value =~ s/\s+$//;
    return $value;
}

sub _urlencode {
    my ( $self, $unencoded ) = @_;
    return uri_escape_utf8( $unencoded, '^A-Za-z0-9_-' );
}

1;
