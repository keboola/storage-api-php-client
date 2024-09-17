<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

trait IpUtils
{
    private static ?string $publicIp = null;

    public function getMyPublicIp(): string
    {
        if (self::$publicIp === null) {
            $curlSession = curl_init();
            curl_setopt($curlSession, CURLOPT_URL, 'http://ifconfig.me/ip');
            curl_setopt($curlSession, CURLOPT_RETURNTRANSFER, true);

            self::$publicIp = curl_exec($curlSession);
            curl_close($curlSession);
        }

        return self::$publicIp;
    }
}
