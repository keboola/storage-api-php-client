<?php

namespace Keboola\StorageApi\Brotli;

use GuzzleHttp\Psr7\NoSeekStream;
use GuzzleHttp\Psr7\Stream;
use GuzzleHttp\Psr7\StreamDecoratorTrait;
use GuzzleHttp\Psr7\StreamWrapper;
use Psr\Http\Message\StreamInterface;

/**
 * Uses Brotli filter to decode content.
 *
 * This stream decorator converts the provided stream to a PHP stream resource,
 * then appends the filter. The stream is then converted back
 * to a Brotli stream resource to be used as a Guzzle stream.
 *
 * @link http://tools.ietf.org/html/rfc7932
 * @link http://php.net/manual/en/filters.compression.php
 *
 * @final
 */
class BrotliStream implements StreamInterface
{
    use StreamDecoratorTrait;

    const FILTER_NAME = 'keboola.brotli';

    /** @var StreamInterface */
    private $stream;

    public function __construct(StreamInterface $stream)
    {
        $resource = StreamWrapper::getResource($stream);
        if (!in_array(self::FILTER_NAME, stream_get_filters())) {
            stream_filter_register(self::FILTER_NAME, BrotliStreamFilter::class);
        }
        stream_filter_append($resource, self::FILTER_NAME, STREAM_FILTER_READ);
        $this->stream = $stream->isSeekable() ? new Stream($resource) : new NoSeekStream(new Stream($resource));
    }
}
