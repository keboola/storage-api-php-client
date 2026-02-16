<?php

namespace Keboola\StorageApi\Downloader;

use InvalidArgumentException;

final class AbsUrlParser
{
    /**
     * @param string $url
     * @return array{0: string, 1: string, 2: string, 3: string}
     */
    public static function parseAbsUrl($url)
    {
        $matched = [];
        if (preg_match(
            '/^(https|azure):\/\/'
                . '(.*?)' // account
                . '\.blob\.core\.windows\.net\/'
                . '(.*?)' // container
                . '\/'
                . '(.*)$/', // filepath
            $url,
            $matched,
        ) === 1 && count($matched) === 5
        ) {
            [$full, $protocol, $account, $container, $file] = $matched;
            return [$protocol, $account, $container, $file];
        }

        // Handle the case where the match is not successful
        throw new InvalidArgumentException('The provided URL does not match the expected format.');
    }
}
