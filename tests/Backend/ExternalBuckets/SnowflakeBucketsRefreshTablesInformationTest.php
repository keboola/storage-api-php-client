<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Throwable;

class SnowflakeBucketsRefreshTablesInformationTest extends BaseExternalBuckets
{
    public function testRefreshTablesInformationEndpointExists(): void
    {
        $bucketId = $this->initEmptyBucket('rti-bucket', 'in', 'refresh-tables-information-bucket');
        $bucket = $this->_client->getBucket($bucketId);

        $this->_client->refreshTableInformationInBucket($bucket['idBranch'], $bucketId);
        $this->assertTrue(true, 'Testing only if requests working.');

        $this->dropBucketIfExists($this->_client, $bucketId);
    }
}
