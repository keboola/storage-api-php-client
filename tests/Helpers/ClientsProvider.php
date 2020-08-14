<?php

namespace Keboola\Test\Helpers;

class ClientsProvider
{
    public static function getGuestStorageApiClient()
    {
        return new \Keboola\StorageApi\Client([
            'token' => STORAGE_API_GUEST_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    public static function getClientForToken($token)
    {
        $client = new \Keboola\StorageApi\Client([
            'token' => $token,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
        return $client;
    }

    public static function getClient()
    {
        return new \Keboola\StorageApi\Client(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ));
    }
}
