<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use Keboola\StorageApi\Client;

class SharingUtils
{
    public static function getSharedBucket(Client $client, string $bucketId, ?int $projectId = null): ?array
    {
        if ($projectId === null) {
            $projectId = (int) explode('-', $client->token)[0];
        }

        $response = $client->listSharedBuckets();
        $sharedBucket = array_values(array_filter($response, function ($sharedBucket) use ($bucketId, $projectId) {
            return $sharedBucket['id'] === $bucketId && $sharedBucket['project']['id'] === $projectId;
        }));
        if (count($sharedBucket) === 0) {
            return null;
        }
        return $sharedBucket[0];
    }
}
