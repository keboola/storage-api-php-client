<?php

declare(strict_types=1);

// Test fixture for BlobClientFactoryTest: answers every blob GET with a 256 KiB Content-Length.
// Sends $argv[2] bytes of it (64 KiB by default, i.e. a body it never fulfils), then stops
// sending for $argv[1] seconds and closes. Send the full length for a healthy response.
// Serves connections one after another so a retried download reaches it too; $argv[3], when
// given, is a file the fixture appends one line to per accepted connection, which is how a test
// counts attempts.
// Prints the port it listens on so the test does not have to guess a free one.

const BLOB_SIZE_BYTES = 262144;

$stallSeconds = (int) ($argv[1] ?? 10);
$bodyBytes = (int) ($argv[2] ?? 65536);
$attemptLog = $argv[3] ?? null;

$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
if ($server === false) {
    fwrite(STDERR, sprintf('cannot listen: %s', $errstr));
    exit(1);
}

$address = stream_socket_get_name($server, false);
if ($address === false) {
    fwrite(STDERR, 'cannot read the listening address');
    exit(1);
}
printf("PORT=%s\n", substr($address, (int) strrpos($address, ':') + 1));

while (($connection = @stream_socket_accept($server, 30)) !== false) {
    if ($attemptLog !== null) {
        file_put_contents($attemptLog, "attempt\n", FILE_APPEND);
    }

    while (($line = fgets($connection)) !== false) {
        if (trim($line) === '') {
            break;
        }
    }

    fwrite($connection, implode("\r\n", [
        'HTTP/1.1 200 OK',
        'Content-Length: ' . BLOB_SIZE_BYTES,
        'Content-Type: application/octet-stream',
        'Last-Modified: Mon, 01 Sep 2025 00:00:00 GMT',
        'ETag: "0x0"',
        'x-ms-blob-type: BlockBlob',
        'x-ms-request-id: stalling-blob-server',
        'Connection: close',
        '',
        '',
    ]));
    fwrite($connection, str_repeat('x', $bodyBytes));

    $deadline = microtime(true) + ($bodyBytes >= BLOB_SIZE_BYTES ? 0 : $stallSeconds);
    // Stop early once the client has given up, so the next attempt is not queued behind this sleep.
    while (microtime(true) < $deadline) {
        $read = [$connection];
        $write = null;
        $except = null;
        if (stream_select($read, $write, $except, 0, 100000) > 0
            && fread($connection, 1) === ''
            && feof($connection)
        ) {
            break;
        }
    }

    fclose($connection);
}
