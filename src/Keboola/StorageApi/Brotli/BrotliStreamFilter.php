<?php

namespace Keboola\StorageApi\Brotli;

class BrotliStreamFilter extends \php_user_filter
{
    public function filter($in, $out, &$consumed, $closing)
    {
        while ($bucket = stream_bucket_make_writeable($in)) {
            $bucket->data = \Brotli\uncompress($bucket->data);
            $consumed += $bucket->datalen;
            stream_bucket_append($out, $bucket);
        }
        return PSFS_PASS_ON;
    }
}
