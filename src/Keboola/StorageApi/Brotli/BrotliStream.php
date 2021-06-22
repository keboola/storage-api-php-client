<?php

namespace Keboola\StorageApi\Brotli;

use GuzzleHttp\Psr7\NoSeekStream;
use GuzzleHttp\Psr7\Stream;
use GuzzleHttp\Psr7\StreamDecoratorTrait;
use GuzzleHttp\Psr7\StreamWrapper;
use Psr\Http\Message\StreamInterface;

/**
 * Uses PHP's zlib.inflate filter to inflate deflate or gzipped content.
 *
 * This stream decorator skips the first 10 bytes of the given stream to remove
 * the gzip header, converts the provided stream to a PHP stream resource,
 * then appends the zlib.inflate filter. The stream is then converted back
 * to a Guzzle stream resource to be used as a Guzzle stream.
 *
 * @link http://tools.ietf.org/html/rfc1952
 * @link http://php.net/manual/en/filters.compression.php
 *
 * @final
 */
class BrotliStream implements StreamInterface
{
    use StreamDecoratorTrait;

    const FILTER_NAME = 'keboola.brotli';

    public function __construct(StreamInterface $stream)
    {
        // read the first 10 bytes, ie. gzip header
        $resource = StreamWrapper::getResource($stream);
        if (!in_array(self::FILTER_NAME, stream_get_filters())) {
            stream_filter_register(self::FILTER_NAME, BrotliStreamFilter::class);
        }
        stream_filter_append($resource, self::FILTER_NAME, STREAM_FILTER_READ);
        $this->stream = $stream->isSeekable() ? new Stream($resource) : new NoSeekStream(new Stream($resource));
    }

    /**
     * @param StreamInterface $stream
     * @param $header
     *
     * @return int
     */
    private function getLengthOfPossibleFilenameHeader(StreamInterface $stream, $header)
    {
        $filename_header_length = 0;

        if (substr(bin2hex($header), 6, 2) === '08') {
            // we have a filename, read until nil
            $filename_header_length = 1;
            while ($stream->read(1) !== chr(0)) {
                $filename_header_length++;
            }
        }

        return $filename_header_length;
    }
}
