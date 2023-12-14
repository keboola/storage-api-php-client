<?php

namespace Keboola\StorageApi\Downloader;

final class AbsUrlParser
{
    /**
     * @param string $url
     * @return array
     */
    public static function parseAbsUrl($url)
    {
        $matched = [];
        preg_match(
            '/^(https|azure):\/\/'
            . '(.*?)' // account
            . '\.blob\.core\.windows\.net\/'
            . '(.*?)' // container
            . '\/'
            . '(.*)$/', // filepath
            $url,
            $matched,
        );
        list($full, $protocol, $account, $container, $file) = $matched;

        return [$protocol, $account, $container, $file];
    }
}
