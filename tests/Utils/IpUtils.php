<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use RuntimeException;

trait IpUtils
{
    private static ?string $publicIp = null;

    public function getMyPublicIp(): string
    {
        if (self::$publicIp === null) {
            $curlSession = curl_init();
            curl_setopt($curlSession, CURLOPT_URL, 'http://ifconfig.me/ip');
            curl_setopt($curlSession, CURLOPT_RETURNTRANSFER, true);

            $response = curl_exec($curlSession);
            if ($response === false || !is_string($response)) {
                throw new RuntimeException(sprintf('Curl error: %s', curl_error($curlSession)));
            }
            self::$publicIp = $response;
            curl_close($curlSession);
        }

        return self::$publicIp;
    }
}
